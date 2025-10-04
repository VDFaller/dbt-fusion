use crate::auth::Auth;
use crate::base_adapter::backend_of;
use crate::config::AdapterConfig;
use crate::databricks::databricks_compute_from_state;
use crate::errors::{AdapterError, AdapterErrorKind, AdapterResult};
use crate::query_comment::{EMPTY_CONFIG, QueryCommentConfig};
use crate::record_and_replay::{RecordEngine, ReplayEngine};
use crate::sql_types::{NaiveTypeFormatterImpl, TypeFormatter};
use crate::stmt_splitter::StmtSplitter;
use crate::{AdapterResponse, TrackedStatement};

use adbc_core::options::{OptionStatement, OptionValue};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow_schema::Schema;
use core::result::Result;
use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::{Cancellable, CancellationToken, never_cancels};
use dbt_common::constants::EXECUTING;
use dbt_common::create_debug_span;
use dbt_common::hashing::code_hash;
use dbt_common::tracing::span_info::record_current_span_status_from_attrs;
use dbt_frontend_common::dialect::Dialect;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::telemetry::{QueryExecuted, QueryOutcome};
use dbt_xdbc::semaphore::Semaphore;
use dbt_xdbc::{Backend, Connection, Database, QueryCtx, Statement, connection, database, driver};
use log;
use minijinja::State;
use serde_json::json;
use std::borrow::Cow;
use tracy_client::span;

use std::collections::HashMap;
use std::fmt::Write;
use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::{Arc, LazyLock};
use std::{thread, time::Duration};

pub type Options = Vec<(String, OptionValue)>;

/// Naive statement splitter used in the MockAdapter
///
/// IMPORTANT: not suitable for production use.
/// TODO: remove when the full stmt splitter is available to this crate.
static NAIVE_STMT_SPLITTER: LazyLock<Arc<dyn StmtSplitter>> =
    LazyLock::new(|| Arc::new(crate::stmt_splitter::NaiveStmtSplitter));

/// Naive type formatter used in the MockAdapter
///
/// IMPORTANT: not suitable for production use. DEFAULTS TO SNOWFLAKE ALSO.
/// TODO: remove when the full formatter is available to this crate.
static NAIVE_TYPE_FORMATTER: LazyLock<Box<dyn TypeFormatter>> =
    LazyLock::new(|| Box::new(NaiveTypeFormatterImpl::new(AdapterType::Snowflake)));

#[derive(Default)]
struct IdentityHasher {
    hash: u64,
    #[cfg(debug_assertions)]
    unexpected_call: bool,
}
impl Hasher for IdentityHasher {
    fn write(&mut self, _bytes: &[u8]) {
        #[cfg(debug_assertions)]
        {
            self.unexpected_call = true;
        }
    }
    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }
    fn finish(&self) -> u64 {
        #[cfg(debug_assertions)]
        {
            debug_assert!(!self.unexpected_call);
        }
        self.hash
    }
}

#[derive(Default)]
struct IdentityBuildHasher;
impl BuildHasher for IdentityBuildHasher {
    type Hasher = IdentityHasher;
    fn build_hasher(&self) -> Self::Hasher {
        IdentityHasher::default()
    }
}

#[derive(Default)]
pub struct DatabaseMap {
    inner: HashMap<database::Fingerprint, Box<dyn Database>, IdentityBuildHasher>,
}

pub struct NoopConnection;

impl Connection for NoopConnection {
    fn new_statement(&mut self) -> adbc_core::error::Result<Box<dyn Statement>> {
        unimplemented!("ADBC statement creation in mock connection")
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        unimplemented!("ADBC connection cancellation in mock connection")
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        unimplemented!("ADBC transaction commit in mock connection")
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        unimplemented!("ADBC transaction rollback in mock connection")
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> adbc_core::error::Result<Schema> {
        unimplemented!("ADBC table schema retrieval in mock connection")
    }
}

pub struct ActualEngine {
    adapter_type: AdapterType,
    /// Auth configurator
    auth: Arc<dyn Auth>,
    /// Configuration
    config: AdapterConfig,
    /// Lazily initialized databases
    configured_databases: RwLock<DatabaseMap>,
    /// Semaphore for limiting the number of concurrent connections
    semaphore: Arc<Semaphore>,
    /// Resolved quoting policy
    quoting: ResolvedQuoting,
    /// Statement splitter
    splitter: Arc<dyn StmtSplitter>,
    /// Query comment config
    query_comment: QueryCommentConfig,
    /// Type formatter for the dilect this engine is for
    pub type_formatter: Box<dyn TypeFormatter>,
    /// Global CLI cancellation token
    cancellation_token: CancellationToken,
}

impl ActualEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        adapter_type: AdapterType,
        auth: Arc<dyn Auth>,
        config: AdapterConfig,
        quoting: ResolvedQuoting,
        splitter: Arc<dyn StmtSplitter>,
        query_comment: QueryCommentConfig,
        type_formatter: Box<dyn TypeFormatter>,
        token: CancellationToken,
    ) -> Self {
        let threads = config
            .get("threads")
            .and_then(|t| {
                let u = t.as_u64();
                debug_assert!(u.is_some(), "threads must be an integer if specified");
                u
            })
            .map(|t| t as u32)
            .unwrap_or(0u32);

        let permits = if threads > 0 { threads } else { u32::MAX };
        Self {
            adapter_type,
            auth,
            config,
            quoting,
            configured_databases: RwLock::new(DatabaseMap::default()),
            semaphore: Arc::new(Semaphore::new(permits)),
            splitter,
            type_formatter,
            query_comment,

            cancellation_token: token,
        }
    }

    fn adapter_type(&self) -> AdapterType {
        self.adapter_type
    }

    fn load_driver_and_configure_database(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Database>> {
        // Delegate the configuration of the database::Builder to the Auth implementation.
        let builder = self.auth.configure(config)?;

        // The driver is loaded only once even if this runs multiple times.
        let mut driver = driver::Builder::new(self.auth.backend())
            .with_semaphore(self.semaphore.clone())
            .try_load()?;

        // builder.with_named_option(
        //     snowflake::LOG_TRACING,
        //     database::LogLevel::Debug.to_string(),
        // )?;
        // ... other configuration steps can be added here...

        // The database is configured only once even if this runs multiple times,
        // unless a different configuration is provided.
        let opts = builder.into_iter().collect::<Vec<_>>();
        let fingerprint = database::Builder::fingerprint(opts.iter());
        {
            let read_guard = self.configured_databases.read().unwrap();
            if let Some(database) = read_guard.inner.get(&fingerprint) {
                return Ok(database.clone());
            }
        }
        {
            let mut write_guard = self.configured_databases.write().unwrap();
            if let Some(database) = write_guard.inner.get(&fingerprint) {
                let database: Box<dyn Database> = database.clone();
                Ok(database)
            } else {
                let database = driver.new_database_with_opts(opts)?;
                write_guard.inner.insert(fingerprint, database.clone());
                Ok(database)
            }
        }
    }

    fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        let mut database = self.load_driver_and_configure_database(config)?;
        let connection_builder = connection::Builder::default();
        let conn = connection_builder.build(&mut database)?;
        Ok(conn)
    }

    fn new_connection(
        &self,
        state: Option<&State>,
        _node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        match self.adapter_type() {
            AdapterType::Databricks => {
                if let Some(databricks_compute) = state.and_then(databricks_compute_from_state) {
                    let augmented_config = {
                        let mut mapping = self.config.repr().clone();
                        mapping.insert("databricks_compute".into(), databricks_compute.into());
                        AdapterConfig::new(mapping)
                    };
                    self.new_connection_with_config(&augmented_config)
                } else {
                    self.new_connection_with_config(&self.config)
                }
            }
            _ => {
                // TODO(felipecrv): Make this codepath more efficient
                // (no need to reconfigure the default database)
                self.new_connection_with_config(&self.config)
            }
        }
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}

/// A simple bridge between adapters and the drivers.
#[derive(Clone)]
pub enum SqlEngine {
    /// Actual engine
    Warehouse(Arc<ActualEngine>),
    /// Engine used for recording db interaction; recording engine is
    /// a wrapper around an actual engine
    Record(RecordEngine),
    /// Engine used for replaying db interaction
    Replay(ReplayEngine),
    /// Mock engine for the MockAdapter
    Mock(AdapterType),
}

impl SqlEngine {
    /// Create a new [`SqlEngine::Warehouse`] based on the given configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        adapter_type: AdapterType,
        auth: Arc<dyn Auth>,
        config: AdapterConfig,
        quoting: ResolvedQuoting,
        stmt_splitter: Arc<dyn StmtSplitter>,
        query_comment: QueryCommentConfig,
        type_formatter: Box<dyn TypeFormatter>,
        token: CancellationToken,
    ) -> Arc<Self> {
        let engine = ActualEngine::new(
            adapter_type,
            auth,
            config,
            quoting,
            stmt_splitter,
            query_comment,
            type_formatter,
            token,
        );
        Arc::new(SqlEngine::Warehouse(Arc::new(engine)))
    }

    /// Create a new [`SqlEngine::Replay`] based on the given path and adapter type.
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_replaying(
        adapter_type: AdapterType,
        path: PathBuf,
        config: AdapterConfig,
        quoting: ResolvedQuoting,
        stmt_splitter: Arc<dyn StmtSplitter>,
        query_comment: QueryCommentConfig,
        type_formatter: Box<dyn TypeFormatter>,
        token: CancellationToken,
    ) -> Arc<Self> {
        let engine = ReplayEngine::new(
            adapter_type,
            path,
            config,
            quoting,
            stmt_splitter,
            query_comment,
            type_formatter,
            token,
        );
        Arc::new(SqlEngine::Replay(engine))
    }

    /// Create a new [`SqlEngine::Record`] wrapping the given engine.
    pub fn new_for_recording(path: PathBuf, engine: Arc<SqlEngine>) -> Arc<Self> {
        let engine = RecordEngine::new(path, engine);
        Arc::new(SqlEngine::Record(engine))
    }

    pub fn is_mock(&self) -> bool {
        matches!(self, SqlEngine::Mock(_))
    }

    pub fn quoting(&self) -> ResolvedQuoting {
        match self {
            SqlEngine::Warehouse(engine) => engine.quoting,
            SqlEngine::Record(engine) => engine.quoting(),
            SqlEngine::Replay(engine) => engine.quoting(),
            SqlEngine::Mock(_) => ResolvedQuoting::default(),
        }
    }

    /// Get the statement splitter for this engine
    pub fn splitter(&self) -> &dyn StmtSplitter {
        match self {
            SqlEngine::Warehouse(engine) => engine.splitter.as_ref(),
            SqlEngine::Record(engine) => engine.splitter(),
            SqlEngine::Replay(engine) => engine.splitter(),
            SqlEngine::Mock(_) => NAIVE_STMT_SPLITTER.as_ref(),
        }
    }

    pub fn type_formatter(&self) -> &dyn TypeFormatter {
        match self {
            SqlEngine::Warehouse(engine) => engine.type_formatter.as_ref(),
            SqlEngine::Record(engine) => engine.type_formatter(),
            SqlEngine::Replay(engine) => engine.type_formatter(),
            SqlEngine::Mock(_adapter_type) => NAIVE_TYPE_FORMATTER.as_ref(),
        }
    }

    /// Split SQL statements using the provided dialect
    ///
    /// This method handles the splitting of SQL statements based on the dialect's rules.
    /// The dialect must be provided by the caller since the mapping from Backend to
    /// AdapterType/Dialect is not always deterministic (e.g., Generic backend,
    /// shared drivers like Postgres/Redshift).
    pub fn split_and_filter_statements(&self, sql: &str, dialect: Dialect) -> Vec<String> {
        self.splitter()
            .split(sql, dialect)
            .into_iter()
            .filter(|statement| !self.splitter().is_empty(statement, dialect))
            .collect()
    }

    /// Get the query comment config for this engine
    pub fn query_comment(&self) -> &QueryCommentConfig {
        match self {
            SqlEngine::Warehouse(engine) => &engine.query_comment,
            SqlEngine::Record(engine) => engine.query_comment(),
            SqlEngine::Replay(engine) => engine.query_comment(),
            SqlEngine::Mock(_) => &EMPTY_CONFIG,
        }
    }

    /// Create a new connection to the warehouse.
    pub fn new_connection_with_config(
        &self,
        config: &AdapterConfig,
    ) -> AdapterResult<Box<dyn Connection>> {
        let _span = span!("ActualEngine::new_connection");
        let conn = match &self {
            Self::Warehouse(actual_engine) => actual_engine.new_connection_with_config(config),
            // TODO: the record and replay engines should have a new_connection_with_config()
            // method instead of a new_connection method
            Self::Record(record_engine) => record_engine.new_connection(None, None),
            Self::Replay(replay_engine) => replay_engine.new_connection(None, None),
            Self::Mock(_) => Ok(Box::new(NoopConnection) as Box<dyn Connection>),
        }?;
        Ok(conn)
    }

    /// Get the adapter type for this engine
    pub fn adapter_type(&self) -> AdapterType {
        match self {
            SqlEngine::Warehouse(actual_engine) => actual_engine.adapter_type(),
            SqlEngine::Record(record_engine) => record_engine.adapter_type(),
            SqlEngine::Replay(replay_engine) => replay_engine.adapter_type(),
            SqlEngine::Mock(adapter_type) => *adapter_type,
        }
    }

    pub fn backend(&self) -> Backend {
        match self {
            SqlEngine::Warehouse(actual_engine) => actual_engine.auth.backend(),
            SqlEngine::Record(record_engine) => record_engine.backend(),
            SqlEngine::Replay(replay_engine) => replay_engine.backend(),
            SqlEngine::Mock(adapter_type) => backend_of(*adapter_type),
        }
    }

    /// Create a new connection to the warehouse.
    pub fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        match &self {
            Self::Warehouse(actual_engine) => actual_engine.new_connection(state, node_id),
            Self::Record(record_engine) => record_engine.new_connection(state, node_id),
            Self::Replay(replay_engine) => replay_engine.new_connection(state, node_id),
            Self::Mock(_) => Ok(Box::new(NoopConnection)),
        }
    }

    /// Execute the given SQL query or statement.
    pub fn execute(
        &self,
        state: Option<&State>,
        conn: &'_ mut dyn Connection,
        query_ctx: &QueryCtx,
    ) -> AdapterResult<RecordBatch> {
        self.execute_with_options(state, query_ctx, conn, Options::new(), true)
    }

    /// Execute the given SQL query or statement.
    pub fn execute_with_options(
        &self,
        state: Option<&State>,
        query_ctx: &QueryCtx,
        conn: &'_ mut dyn Connection,
        options: Options,
        fetch: bool,
    ) -> AdapterResult<RecordBatch> {
        assert!(query_ctx.sql().is_some() || !options.is_empty());

        let query_ctx = if let Some(sql) = query_ctx.sql() {
            if let Some(state) = state {
                &query_ctx.with_sql(self.query_comment().add_comment(state, sql)?)
            } else {
                query_ctx
            }
        } else {
            query_ctx
        };

        Self::log_query_ctx_for_execution(query_ctx);

        let token = self.cancellation_token();
        let do_execute = |conn: &'_ mut dyn Connection| -> Result<
            (Arc<Schema>, Vec<RecordBatch>),
            Cancellable<adbc_core::error::Error>,
        > {
            use dbt_xdbc::statement::Statement as _;

            let mut stmt = conn.new_statement()?;
            stmt.set_sql_query(query_ctx)?;

            options
                .into_iter()
                .try_for_each(|(key, value)| stmt.set_option(OptionStatement::Other(key), value))?;

            // Make sure we don't create more statements after global cancellation.
            token.check_cancellation()?;

            // Track the statement so execution can be cancelled
            // when the user Ctrl-C's the process.
            let mut stmt = TrackedStatement::new(stmt);

            let reader = stmt.execute()?;
            let schema = reader.schema();
            let mut batches = Vec::with_capacity(1);
            if !fetch {
                return Ok((schema, batches));
            }
            for res in reader {
                let batch = res.map_err(adbc_core::error::Error::from)?;
                batches.push(batch);
                // Check for cancellation before processing the next batch
                // or concatenating the batches produced so far.
                token.check_cancellation()?;
            }
            Ok((schema, batches))
        };
        let _span = span!("SqlEngine::execute");

        let sql = query_ctx.sql().unwrap_or_default();
        let sql_hash = code_hash(sql.as_ref());
        let adapter_type = self.adapter_type();
        let _query_span_guard = create_debug_span!(
            QueryExecuted::start(
                sql,
                sql_hash,
                adapter_type.as_ref().to_owned(),
                query_ctx.node_id(),
                query_ctx.desc()
            )
            .into()
        )
        .entered();

        let (schema, batches) = match do_execute(conn) {
            Ok(res) => res,
            Err(Cancellable::Cancelled) => {
                let e = AdapterError::new(
                    AdapterErrorKind::Cancelled,
                    "SQL statement execution was cancelled",
                );

                // TODO: wouldn't it be possible to salvage query_id if at least one batch was produced?
                record_current_span_status_from_attrs(|attrs| {
                    if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                        // dbt core had different event codes for start and end of a query
                        attrs.dbt_core_event_code = "E017".to_string();
                        attrs.set_query_outcome(QueryOutcome::Canceled);
                    }
                });

                return Err(e);
            }
            Err(Cancellable::Error(e)) => {
                // TODO: wouldn't it be possible to salvage query_id if at least one batch was produced?
                record_current_span_status_from_attrs(|attrs| {
                    if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                        // dbt core had different event codes for start and end of a query
                        attrs.dbt_core_event_code = "E017".to_string();
                        attrs.set_query_outcome(QueryOutcome::Error);
                        attrs.query_error_adapter_message =
                            Some(format!("{:?}: {}", e.status, e.message));
                        attrs.query_error_vendor_code = Some(e.vendor_code);
                    }
                });

                return Err(e.into());
            }
        };
        let total_batch = concat_batches(&schema, &batches)?;

        record_current_span_status_from_attrs(|attrs| {
            if let Some(attrs) = attrs.downcast_mut::<QueryExecuted>() {
                // dbt core had different event codes for start and end of a query
                attrs.dbt_core_event_code = "E017".to_string();
                attrs.set_query_outcome(QueryOutcome::Success);
                attrs.query_id = AdapterResponse::query_id(&total_batch, adapter_type)
            }
        });

        Ok(total_batch)
    }

    // TODO: kill this when telemtry starts writing dbt.log
    /// Format query context as we want to see it in a log file and log it in query_log
    pub fn log_query_ctx_for_execution(ctx: &QueryCtx) {
        let mut buf = String::new();

        writeln!(&mut buf, "-- created_at: {}", ctx.created_at_as_str()).unwrap();
        writeln!(&mut buf, "-- dialect: {}", ctx.adapter_type()).unwrap();

        let node_id = match ctx.node_id() {
            Some(id) => id,
            None => "not available".to_string(),
        };
        writeln!(&mut buf, "-- node_id: {node_id}").unwrap();

        match ctx.desc() {
            Some(desc) => writeln!(&mut buf, "-- desc: {desc}").unwrap(),
            None => writeln!(&mut buf, "-- desc: not provided").unwrap(),
        }

        if let Some(sql) = ctx.sql() {
            write!(&mut buf, "{sql}").unwrap();
            if !sql.ends_with(";") {
                write!(&mut buf, ";").unwrap();
            }
        }

        if node_id != "not available" {
            log::debug!(target: EXECUTING, name = "SQLQuery", data:serde = json!({ "node_info": { "unique_id": node_id } }); "{buf}");
        } else {
            log::debug!(target: EXECUTING, name = "SQLQuery"; "{buf}");
        }
    }

    /// Get the configured database name. Used by
    /// adapter.verify_database to check if the database is valid.
    pub fn get_configured_database_name(&self) -> Option<Cow<'_, str>> {
        self.config("database")
    }

    /// Get a config value by key
    ///
    /// ## Returns
    /// always is Ok(None) for non Warehouse/Record variance
    pub fn config(&self, key: &str) -> Option<Cow<'_, str>> {
        match self {
            Self::Warehouse(actual_engine) => actual_engine.config.get_string(key),
            Self::Record(record_engine) => record_engine.config(key),
            Self::Replay(replay_engine) => replay_engine.config(key),
            Self::Mock(_) => None,
        }
    }

    // Get full config object
    pub fn get_config(&self) -> &AdapterConfig {
        match self {
            Self::Warehouse(actual_engine) => &actual_engine.config,
            Self::Record(record_engine) => record_engine.get_config(),
            Self::Replay(replay_engine) => replay_engine.get_config(),
            Self::Mock(_) => unreachable!("Mock engine does not support get_config"),
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        match self {
            Self::Warehouse(actual_engine) => actual_engine.cancellation_token(),
            Self::Record(record_engine) => record_engine.cancellation_token(),
            Self::Replay(replay_engine) => replay_engine.cancellation_token(),
            Self::Mock(_) => never_cancels(),
        }
    }
}

/// Execute query and retry in case of an error. Retry is done (up to
/// the given limit) regardless of the error encountered.
///
/// https://github.com/dbt-labs/dbt-adapters/blob/996a302fa9107369eb30d733dadfaf307023f33d/dbt-adapters/src/dbt/adapters/sql/connections.py#L84
pub fn execute_query_with_retry(
    engine: Arc<SqlEngine>,
    state: Option<&State>,
    conn: &'_ mut dyn Connection,
    query_ctx: &QueryCtx,
    retry_limit: u32,
    options: &Options,
    fetch: bool,
) -> AdapterResult<RecordBatch> {
    let mut attempt = 0;
    let mut last_error = None;

    while attempt < retry_limit {
        match engine.execute_with_options(state, query_ctx, conn, options.clone(), fetch) {
            Ok(result) => return Ok(result),
            Err(err) => {
                last_error = Some(err.clone());
                thread::sleep(Duration::from_secs(1));
                attempt += 1;
            }
        }
    }

    if let Some(err) = last_error {
        Err(err)
    } else {
        unreachable!("last_error should not be None if we exit the loop")
    }
}

#[cfg(test)]
mod tests {
    use dbt_xdbc::QueryCtx;

    use super::SqlEngine;

    #[test]
    fn test_log_for_execution() {
        let query_ctx = QueryCtx::new("test_adapter")
            .with_node_id("test_node_123")
            .with_sql("SELECT * FROM test_table")
            .with_desc("Test query for logging");

        // Should not panic
        SqlEngine::log_query_ctx_for_execution(&query_ctx);
    }
}
