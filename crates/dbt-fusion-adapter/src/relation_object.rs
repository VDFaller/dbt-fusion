use dbt_common::adapter::AdapterType;
use dbt_common::{FsError, FsResult};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::filter::RunFilter;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::common::{DbtQuoting, ResolvedQuoting};
use dbt_schemas::schemas::relations::base::{
    BaseRelation, TableFormat, render_with_run_filter_as_str,
};
use minijinja::arg_utils::ArgParser;
use minijinja::value::{Enumerator, Object, ValueKind};
use minijinja::{Error as MinijinjaError, State};
use minijinja::{Value, listener::RenderingEventListener};
use serde::Deserialize;

use crate::bigquery::relation::BigqueryRelation;
use crate::databricks::relation::DatabricksRelation;
use crate::postgres::relation::PostgresRelation;
use crate::redshift::relation::RedshiftRelation;
use crate::salesforce::relation::SalesforceRelation;
use crate::snowflake::relation::SnowflakeRelation;

use std::sync::Arc;
use std::{fmt, ops::Deref};

/// A Wrapper type for BaseRelation
/// for any concrete Relation type to be used as Object in Jinja
#[derive(Clone)]
pub struct RelationObject {
    relation: Arc<dyn BaseRelation>,
    run_filter: Option<RunFilter>,
    event_time: Option<String>,
}

impl RelationObject {
    pub fn new(relation: Arc<dyn BaseRelation>) -> Self {
        Self {
            relation,
            run_filter: None,
            event_time: None,
        }
    }

    pub fn new_with_filter(
        relation: Arc<dyn BaseRelation>,
        run_filter: RunFilter,
        event_time: Option<String>,
    ) -> Self {
        Self {
            relation,
            run_filter: Some(run_filter),
            event_time,
        }
    }

    pub fn into_value(self) -> Value {
        Value::from_object(self)
    }

    pub fn inner(&self) -> Arc<dyn BaseRelation> {
        self.relation.clone()
    }
}

impl fmt::Debug for RelationObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.render_self().expect("could not render self"))
    }
}

impl Deref for RelationObject {
    type Target = dyn BaseRelation;

    fn deref(&self) -> &Self::Target {
        self.relation.as_ref()
    }
}

impl Object for RelationObject {
    fn call_method(
        self: &Arc<Self>,
        state: &State,
        name: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        match name {
            "create_from" => self.create_from(state, args),
            "replace_path" => self.replace_path(args),
            "get" => self.get(args),
            "render" => self.render_self(),
            "without_identifier" => self.without_identifier(args),
            "include" => self.include(args),
            "incorporate" => self.incorporate(args),
            "information_schema" => self.information_schema(args),
            "relation_max_name_length" => self.relation_max_name_length(args),
            // Below are available for Snowflake
            "get_ddl_prefix_for_create" => self.get_ddl_prefix_for_create(args),
            "get_ddl_prefix_for_alter" => self.get_ddl_prefix_for_alter(),
            "needs_to_drop" => self.needs_to_drop(args),
            "get_iceberg_ddl_options" => self.get_iceberg_ddl_options(args),
            "dynamic_table_config_changeset" => self.dynamic_table_config_changeset(args),
            "from_config" => self.from_config(args),
            // Below are available for Databricks
            "is_hive_metastore" => Ok(self.is_hive_metastore()),
            // Below are available for BigQuery and Redshift
            "materialized_view_config_changeset" => self.materialized_view_config_changeset(args),
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("Unknown method on BaseRelationObject: '{name}'"),
            )),
        }
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        match key.as_str() {
            Some("database") => Some(self.database()),
            Some("schema") => Some(self.schema()),
            Some("identifier") | Some("name") | Some("table") => Some(self.identifier()),
            Some("is_table") => Some(Value::from(self.is_table())),
            Some("is_view") => Some(Value::from(self.is_view())),
            Some("is_materialized_view") => Some(Value::from(self.is_materialized_view())),
            Some("is_streaming_table") => Some(Value::from(self.is_streaming_table())),
            Some("is_dynamic_table") => Some(Value::from(self.is_dynamic_table())),
            Some("is_cte") => Some(Value::from(self.is_cte())),
            Some("is_pointer") => Some(Value::from(self.is_pointer())),
            Some("type") => Some(self.relation_type_as_value()),
            Some("can_be_renamed") => Some(Value::from(self.can_be_renamed())),
            Some("can_be_replaced") => Some(Value::from(self.can_be_replaced())),
            Some("MaterializedView") => {
                Some(Value::from(RelationType::MaterializedView.to_string()))
            }
            Some("Table") => Some(Value::from(RelationType::Table.to_string())),
            Some("DynamicTable") => Some(Value::from(RelationType::DynamicTable.to_string())),
            _ => None,
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        Enumerator::Str(&[
            "database",
            "schema",
            "identifier",
            "is_table",
            "is_view",
            "is_materialized_view",
            "is_streaming_table",
            "is_cte",
            "is_pointer",
            "can_be_renamed",
            "can_be_replaced",
            "name",
        ])
    }

    fn render(self: &Arc<Self>, f: &mut fmt::Formatter<'_>) -> fmt::Result
    where
        Self: Sized + 'static,
    {
        if let Some(run_filter) = &self.run_filter {
            if run_filter.enabled() {
                let rendered = self.render_self_as_str();
                return write!(
                    f,
                    "{}",
                    render_with_run_filter_as_str(rendered, run_filter, &self.event_time)
                );
            }
        }

        write!(f, "{}", self.render_self().expect("could not render self"))
    }
}

/// Creates a relation based on the adapter type
///
/// Unlike [internal_create_relation]
/// This is supposed to be used in places that are invoked by the Jinja rendering process
pub fn create_relation(
    adapter_type: AdapterType,
    database: String,
    schema: String,
    identifier: Option<String>,
    relation_type: Option<RelationType>,
    custom_quoting: ResolvedQuoting,
) -> Result<Arc<dyn BaseRelation>, MinijinjaError> {
    let relation = match adapter_type {
        AdapterType::Postgres => Arc::new(PostgresRelation::try_new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            custom_quoting,
        )?) as Arc<dyn BaseRelation>,
        AdapterType::Snowflake => Arc::new(SnowflakeRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            TableFormat::Default,
            custom_quoting,
        )) as Arc<dyn BaseRelation>,
        AdapterType::Bigquery => Arc::new(BigqueryRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
        )) as Arc<dyn BaseRelation>,
        AdapterType::Redshift => Arc::new(RedshiftRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
        )) as Arc<dyn BaseRelation>,
        AdapterType::Databricks => Arc::new(DatabricksRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
            None,
            custom_quoting,
            None,
            false,
        )) as Arc<dyn BaseRelation>,
        AdapterType::Salesforce => Arc::new(SalesforceRelation::new(
            Some(database),
            Some(schema),
            identifier,
            relation_type,
        )) as Arc<dyn BaseRelation>,
        AdapterType::Parse => panic!("Adapter type not supported: {adapter_type}"),
    };
    Ok(relation)
}

/// Creates a relation based on the adapter type
///
/// This is a wrapper around the [create_relation] function
/// that is supposed to be used outside the context of Jinja
pub fn create_relation_internal(
    adapter_type: AdapterType,
    database: String,
    schema: String,
    identifier: Option<String>,
    relation_type: Option<RelationType>,
    custom_quoting: ResolvedQuoting,
) -> FsResult<Arc<dyn BaseRelation>> {
    let result = create_relation(
        adapter_type,
        database,
        schema,
        identifier,
        relation_type,
        custom_quoting,
    )
    .map_err(|e| FsError::from_jinja_err(e, "Failed to create relation"))?;
    Ok(result)
}

pub fn create_relation_from_node(
    adapter_type: AdapterType,
    node: &dyn InternalDbtNodeAttributes,
    _sample_config: Option<RunFilter>,
) -> FsResult<Arc<dyn BaseRelation>> {
    create_relation_internal(
        adapter_type,
        node.database(),
        node.schema(),
        Some(node.base().alias.clone()), // all identifiers are consolidated to alias in InternalDbtNode
        Some(RelationType::from(node.materialized())),
        node.quoting(),
    )
}

/// A Wrapper type for StaticBaseRelation
/// for any concrete StaticBaseRelation type to be used as Object in Jinja
/// to expose static methods via api.Relation
#[derive(Debug, Clone)]
pub struct StaticBaseRelationObject(Arc<dyn StaticBaseRelation>);

impl StaticBaseRelationObject {
    pub fn new(relation: Arc<dyn StaticBaseRelation>) -> Self {
        Self(relation)
    }
}

impl Deref for StaticBaseRelationObject {
    type Target = dyn StaticBaseRelation;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl Object for StaticBaseRelationObject {
    fn call_method(
        self: &Arc<Self>,
        _state: &State,
        name: &str,
        args: &[Value],
        _listeners: &[std::rc::Rc<dyn RenderingEventListener>],
    ) -> Result<Value, MinijinjaError> {
        match name {
            "create" => self.create(args),
            "scd_args" => self.scd_args(args),
            _ => Err(minijinja::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("Unknown method on StaticBaseRelationObject: '{name}'"),
            )),
        }
    }
}

/// Trait for static methods on relations
pub trait StaticBaseRelation: fmt::Debug + Send + Sync {
    /// Create a new relation from the given arguments
    fn try_new(
        &self,
        database: Option<String>,
        schema: Option<String>,
        identifier: Option<String>,
        relation_type: Option<RelationType>,
        custom_quoting: Option<ResolvedQuoting>,
    ) -> Result<Value, MinijinjaError>;

    fn get_adapter_type(&self) -> String;

    /// Create a new relation from the given arguments
    /// impl for api.Relation.create
    fn create(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let database: Option<String> = args.get("database").ok();
        let schema: Option<String> = args.get("schema").ok();
        let identifier: Option<String> = args.get("identifier").ok();
        let relation_type: Option<String> = args.get("type").ok();
        let custom_quoting: Option<Value> = args.get("quote_policy").ok();

        // error is intentionally silenced
        let custom_quoting = custom_quoting
            .and_then(|v| DbtQuoting::deserialize(v).ok())
            // when missing, defaults to be non-quoted
            .map(|v| ResolvedQuoting {
                database: v.database.unwrap_or_default(),
                identifier: v.identifier.unwrap_or_default(),
                schema: v.schema.unwrap_or_default(),
            });

        self.try_new(
            database,
            schema,
            identifier,
            relation_type.map(|s: String| RelationType::from(s.as_str())),
            custom_quoting,
        )
    }

    /// Get the SCD arguments for the relation
    fn scd_args(&self, args: &[Value]) -> Result<Value, MinijinjaError> {
        let mut args = ArgParser::new(args, None);
        let primary_key: Value = args.get("primary_key").unwrap();
        let updated_at: String = args.get("updated_at").unwrap();
        let mut scd_args = vec![];
        // Check if minijinja value is a vector
        match primary_key.kind() {
            ValueKind::Seq => {
                scd_args.extend(primary_key.try_iter()?.enumerate().map(|s| s.1.to_string()));
            }
            ValueKind::String => {
                scd_args.push(primary_key.as_str().unwrap().to_string());
            }
            _ => {
                return Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    format!(
                        "'primary_key' has a wrong type in StaticBaseRelationObject: '{primary_key}'"
                    ),
                ));
            }
        }
        scd_args.push(updated_at);
        Ok(Value::from(scd_args))
    }
}
