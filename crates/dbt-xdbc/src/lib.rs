#![cfg_attr(docsrs, feature(doc_auto_cfg, doc_cfg))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/docs/source/_static/favicon.ico",
    html_favicon_url = "https://raw.githubusercontent.com/apache/arrow/refs/heads/main/docs/source/_static/favicon.ico"
)]
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]
#![allow(clippy::cognitive_complexity)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::let_and_return)]
#![allow(clippy::needless_bool)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::should_implement_trait)]

use dbt_cancel::{Cancellable, CancellationToken, CancelledError};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinError;
use tracy_client::span;

use std::ffi::c_char;
use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

pub mod driver;
pub use driver::Backend;
pub use driver::Driver;

pub mod database;
pub use database::Database;

pub mod connection;
pub use connection::Connection;

pub mod statement;
pub use statement::Statement;

pub mod query_ctx;
pub use query_ctx::QueryCtx;

pub mod semaphore;
pub mod sql;

#[cfg(feature = "odbc")]
pub(crate) mod odbc;
#[cfg(feature = "odbc")]
pub(crate) mod odbc_api;

pub(crate) mod builder;
pub(crate) mod checksums;
pub mod duration;
pub(crate) mod install;

// Constants for different backends
pub mod bigquery;
pub mod databricks;
pub mod redshift;
pub mod salesforce;
pub mod snowflake;

// REPL for ADBC drivers
#[cfg(feature = "repl")]
pub mod repl;

/// Interpret the SQLSTATE [1] 5-char ASCII string as a Rust string.
///
/// [1] https://en.wikipedia.org/wiki/SQLSTATE
pub fn str_from_sqlstate(sqlstate: &[c_char; 5]) -> &str {
    // This is safe because the range of the byte values is validated by str::from_utf8 below.
    // It would be unnecessary if Rust ADBC used u8 for [`Error::sqlstate`] [1] instead of i8.
    //
    // [1] https://github.com/apache/arrow-adbc/pull/1725#discussion_r1567531539
    let unsigned: &[u8; 5] = unsafe { std::mem::transmute(sqlstate) };
    let res = std::str::from_utf8(unsigned);
    debug_assert!(res.is_ok(), "SQLSTATE is not valid ASCII: {sqlstate:?}");
    res.unwrap_or("")
}

// XXX: if needed, rollback to 0.17.0+dbt0.0.8 because 0.0.9 is broken on Windows
pub const SNOWFLAKE_DRIVER_VERSION: &str = "0.18.0+dbt0.0.17";
pub const BIGQUERY_DRIVER_VERSION: &str = "0.18.0+dbt0.0.18";
pub const POSTGRES_DRIVER_VERSION: &str = "0.18.0+dbt0.0.3";
pub const DATABRICKS_DRIVER_VERSION: &str = "0.18.0+dbt0.0.6";
pub const REDSHIFT_DRIVER_VERSION: &str = "0.18.0+dbt0.18.2";
pub const SALESFORCE_DRIVER_VERSION: &str = "0.18.0+dbt0.0.4";

pub use install::pre_install_all_drivers;
pub use install::pre_install_driver;

/// A function that creates a new connection to the database.
type NewConnectionF<Error> = Box<dyn Fn() -> Result<Box<dyn Connection>, Error> + Send + Sync>;

/// A function that maps a key to a computed value using a [Connection].
type MapF<Key, Value> = Box<dyn Fn(&'_ mut dyn Connection, &Key) -> Value + Send + Sync>;

/// A function that reduces a computed value into an accumulator.
type ReduceF<Acc, Key, Value, Error> =
    Box<dyn Fn(&mut Acc, Key, Value) -> Result<(), Error> + Send + Sync>;

struct MapReduceInner<Key, Value, Acc, Error>
where
    Key: Sized + Send,
    Value: Sized + Send + 'static,
    Acc: Sized + Default + Send + 'static,
    Error: Send,
{
    /// Function to create a new connection.
    new_connection_f: NewConnectionF<Cancellable<Error>>,
    /// Function to map a key to a computed value using a [Connection].
    map_f: MapF<Key, Value>,
    /// Function to reduce a computed value into the accumulator.
    reduce_f: ReduceF<Acc, Key, Value, Cancellable<Error>>,

    /// The next key to be processed by any of the workers.
    key_counter: AtomicUsize,
    /// Total time spent in `task_count` tasks.
    total_task_time_us: AtomicU64,
    task_count: AtomicU64,
    /// Total time spent in `conn_count` connections.
    total_conn_time_us: AtomicU64,
    conn_count: AtomicU64,
}

impl<K, V, Acc, E> MapReduceInner<K, V, Acc, E>
where
    K: Sized + Send,
    V: Sized + Send + 'static,
    Acc: Sized + Default + Send + 'static,
    E: Send + 'static,
{
    #[inline(never)]
    fn new_connection(&self) -> Result<Box<dyn Connection>, Cancellable<E>> {
        let _span = span!("MapReduceInner::new_connection");
        let start = std::time::Instant::now();
        let res = (self.new_connection_f)();
        if res.is_ok() {
            let elapsed = start.elapsed();
            self.conn_count.fetch_add(1, Ordering::SeqCst);
            self.total_conn_time_us
                .fetch_add(elapsed.as_micros() as u64, Ordering::SeqCst);
        }
        res
    }

    fn map(&self, conn: &'_ mut dyn Connection, key: &K) -> V {
        let _span = span!("MapReduceInner::map");
        let start = std::time::Instant::now();
        let res = (self.map_f)(conn, key);
        let elapsed = start.elapsed();
        self.task_count.fetch_add(1, Ordering::SeqCst);
        self.total_task_time_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::SeqCst);
        res
    }

    fn avg_conn_time_us(&self) -> f64 {
        let conn_count = self.conn_count.load(Ordering::SeqCst);
        self.total_conn_time_us.load(Ordering::SeqCst) as f64 / conn_count.max(1) as f64
    }

    fn avg_task_time_us(&self) -> f64 {
        // if an older task_count or total_task_time_us is loaded, the
        // average will be incorrect, but the error will be small
        let task_count = self.task_count.load(Ordering::SeqCst);
        self.total_task_time_us.load(Ordering::SeqCst) as f64 / task_count.max(1) as f64
    }
}

/// Run parallel Key-to-Value tasks in parallel with a bounded number of
/// connections and reduce the results into an accumulator.
///
/// Example:
///
/// ```rust
/// type Acc = HashMap<String, AdapterResult<Schema>>;
/// # let adapter = self.clone(); // clone needed to move it into lambda
/// let new_connection_f = Box::new(move || adapter.new_connection());
/// # let adapter = self.clone();
/// let map_f =
///     move |conn: &'_ mut dyn Connection, table_name: &String| -> AdapterResult<Schema> {
///         let sql = format!("SHOW COLUMNS IN TABLE {};", &table_name);
///         let (_, table) = adapter.execute(conn, &sql, None, None, None)?;
///         let batch = table.to_record_batch();
///         let schema = build_schema_from(batch)?;
///         Ok(schema)
///     };
/// let reduce_f = |acc: &mut Acc, table_name: String, schema: AdapterResult<Schema>| {
///     acc.insert(table_name, schema);
/// };
/// let map_reduce = MapReduce::new(
///     Box::new(new_connection_f),
///     Box::new(map_f),
///     Box::new(reduce_f),
///     MAX_CONNECTIONS,
/// );
/// let table_names = relations
///     .iter()
///     .map(|relation| relation.render_self_as_str());
/// map_reduce.run(table_names).await
/// ```
pub struct MapReduce<Key, Value, Acc, Error>
where
    Key: Sized + Clone + Send + Sync + 'static,
    Value: Sized + Send + 'static,
    Acc: Sized + Default + Send + 'static,
    Error: Send + 'static,
{
    inner: Arc<MapReduceInner<Key, Value, Acc, Error>>,
    max_connections: usize,
}

impl<K, V, Acc, E> MapReduce<K, V, Acc, E>
where
    K: Sized + Clone + Send + Sync + 'static,
    V: Sized + Send + 'static,
    Acc: Sized + Default + Send + 'static,
    E: Send + 'static,
{
    pub fn new(
        new_connection_f: NewConnectionF<Cancellable<E>>,
        map_f: MapF<K, V>,
        reduce_f: ReduceF<Acc, K, V, Cancellable<E>>,
        max_connections: usize,
    ) -> Self {
        let inner = MapReduceInner {
            new_connection_f,
            map_f,
            reduce_f,
            key_counter: AtomicUsize::new(0),
            total_task_time_us: AtomicU64::new(0),
            task_count: AtomicU64::new(0),
            total_conn_time_us: AtomicU64::new(0),
            conn_count: AtomicU64::new(0),
        };
        Self {
            inner: Arc::new(inner),
            max_connections: max_connections.max(2),
        }
    }

    #[inline(never)]
    #[allow(clippy::type_complexity)]
    pub fn new_connection(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Connection>, Cancellable<E>>> + Send>> {
        let inner = self.inner.clone(); // clone needed to move it into lambda
        let future = async move {
            match tokio::task::spawn_blocking(move || inner.new_connection()).await {
                Ok(res) => res,
                Err(join_err) => Err(cancellable_from_join_error(join_err)),
            }
        };
        Box::pin(future)
    }

    #[inline(never)]
    #[allow(clippy::type_complexity)]
    fn worker(
        &self,
        conn: Box<dyn Connection>,
        tx: mpsc::UnboundedSender<(K, V)>,
        keys: Arc<Vec<K>>,
        token: &CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), CancelledError>> + Send>> {
        let inner = self.inner.clone(); // clone needed to move it into lambda
        let token = token.clone(); // clone needed to move it into lambda
        let future = async move {
            let mut conn = conn;
            loop {
                let inner = inner.clone();
                let keys_for_task = keys.clone();
                let i = inner.key_counter.fetch_add(1, Ordering::SeqCst);
                if i >= keys.len() {
                    return Ok(());
                }
                let handle = tokio::task::spawn_blocking(move || {
                    let key = &keys_for_task[i];
                    let value = inner.map(&mut *conn, key);
                    (conn, value)
                });
                // unwrap() fails only when the task code above panics, so calling
                // it makes the code no more panic-prone than it alerady is
                let conn_value = match handle.await {
                    Ok(conn_value) => conn_value,
                    Err(join_error) => {
                        let err = cancelled_from_join_error(join_error);
                        return Err(err);
                    }
                };
                conn = conn_value.0;
                let value = conn_value.1;

                let key = keys[i].clone();
                match tx.send((key, value)) {
                    Ok(()) => (),
                    Err(SendError(_)) => {
                        // The receiver has been dropped (due to cancellation),
                        // so we fail with a CancelledError.
                        return Err(CancelledError);
                    }
                }

                if token.is_cancelled() {
                    return Err(CancelledError);
                }
            }
        };
        Box::pin(future)
    }

    /// Reduce a computed value into an accumulator.
    fn reduce(&self, acc: &mut Acc, key: K, value: V) -> Result<(), Cancellable<E>> {
        (self.inner.reduce_f)(acc, key, value)
    }

    /// Run all tasks in parallel with at most `max_connections` connections.
    async fn do_run(
        self,
        keys: Arc<Vec<K>>,
        token: CancellationToken,
    ) -> Result<Acc, Cancellable<E>> {
        let mut acc = Acc::default();
        if keys.is_empty() {
            return Ok(acc);
        }

        let mut recv_buffer = Vec::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<(K, V)>();

        let max_conns = keys.len().min(self.max_connections);
        let mut conn_futures = FuturesUnordered::new();
        let mut workers = FuturesUnordered::new();

        let mut n_conns = {
            conn_futures.push(self.new_connection());
            if max_conns > 1 {
                // If we have more than one task, we can start a second
                // connection before knowing how long the tasks will take.
                conn_futures.push(self.new_connection());
                2
            } else {
                1
            }
        };
        // To start, ensure there is at least one connection open and one task enqueued.
        // Even if all the other connections fail, we can still keep making progress by
        // reusing the first connection.
        let conn = conn_futures.next().await.unwrap()?;
        let worker = tokio::spawn(self.worker(conn, tx.clone(), keys.clone(), &token));
        workers.push(worker);

        while self.inner.key_counter.load(Ordering::SeqCst) < keys.len() {
            if let Some(Ok(conn)) = conn_futures.next().await {
                let worker = tokio::spawn(self.worker(conn, tx.clone(), keys.clone(), &token));
                workers.push(worker);
            }
            if n_conns < max_conns {
                let remaining_keys = {
                    let key_counter = self.inner.key_counter.load(Ordering::SeqCst);
                    if key_counter < keys.len() {
                        keys.len() - key_counter
                    } else {
                        0
                    }
                };

                const K: f64 = 1.5; // sensitivity factor
                if (remaining_keys as f64 * self.inner.avg_task_time_us()) / (n_conns as f64)
                    > (self.inner.avg_conn_time_us() * K)
                {
                    conn_futures.push(self.new_connection());
                    n_conns += 1;
                    continue;
                }
            }

            if !rx.is_empty() {
                let n = rx.recv_many(&mut recv_buffer, n_conns).await;
                debug_assert!(recv_buffer.len() == n);
                for _ in 0..n {
                    let (key, value) = recv_buffer.pop().unwrap();
                    self.reduce(&mut acc, key, value)?;
                }
            } else if self.inner.key_counter.load(Ordering::SeqCst) < keys.len() {
                let us = self.inner.avg_conn_time_us().floor() as u64;
                let duration = Duration::from_micros(us).min(Duration::from_secs(1));
                tokio::time::sleep(duration).await;
            }

            token.check_cancellation()?;
        }
        drop(tx);

        // Wait for all the workers to finish...
        while let Some(res) = workers.next().await {
            match res {
                Ok(Ok(())) => (),
                Ok(Err(CancelledError)) => {
                    return Err(CancelledError.into());
                }
                Err(join_error) => {
                    return Err(cancellable_from_join_error(join_error));
                }
            }
            token.check_cancellation()?;
        }
        // ...and reduce their results.
        loop {
            let n = rx.recv_many(&mut recv_buffer, n_conns).await;
            if n == 0 {
                break;
            }
            for _ in 0..n {
                let (key, value) = recv_buffer.pop().unwrap();
                self.reduce(&mut acc, key, value)?;
            }
            token.check_cancellation()?;
        }

        Ok(acc)
    }

    pub fn run(
        self,
        keys: Arc<Vec<K>>,
        token: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Acc, Cancellable<E>>> + Send>> {
        let future = self.do_run(keys, token);
        Box::pin(future)
    }
}

fn cancelled_from_join_error(err: JoinError) -> CancelledError {
    if err.is_cancelled() {
        CancelledError
    } else if err.is_panic() {
        panic::resume_unwind(err.into_panic());
    } else {
        unreachable!("JoinError's are either due to cancellation or panic");
    }
}

fn cancellable_from_join_error<T>(err: JoinError) -> Cancellable<T> {
    cancelled_from_join_error(err).into()
}
