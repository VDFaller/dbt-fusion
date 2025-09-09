use crate::args::ResolveArgs;

use dbt_common::FsResult;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::serde::into_typed_with_jinja;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::common::NodeDependsOn;
use dbt_schemas::schemas::nodes::{CommonAttributes, DbtGroup, DbtGroupAttr, NodeBaseAttributes};
use dbt_schemas::schemas::properties::{GroupConfig, GroupProperties};
use minijinja::value::Value as MinijinjaValue;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use super::resolve_properties::MinimalPropertiesEntry;

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub async fn resolve_groups(
    args: &ResolveArgs,
    group_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    package_name: &str,
    env: &JinjaEnv,
    base_ctx: &BTreeMap<String, MinijinjaValue>,
) -> FsResult<(
    HashMap<String, Arc<DbtGroup>>,
    HashMap<String, Arc<DbtGroup>>,
)> {
    let mut groups: HashMap<String, Arc<DbtGroup>> = HashMap::new();
    let dependency_package_name = dependency_package_name_from_ctx(env, base_ctx);

    // Retrieve groups from yaml
    for (group_name, mpe) in group_properties.iter_mut() {
        if !mpe.schema_value.is_null() {
            let unique_id = format!("group.{}.{}", &package_name, group_name);

            let schema_value =
                std::mem::replace(&mut mpe.schema_value, dbt_serde_yaml::Value::null());
            // GroupProperties is for the yaml schema
            let group: GroupProperties = into_typed_with_jinja(
                &args.io,
                schema_value,
                false,
                env,
                base_ctx,
                &[],
                dependency_package_name,
            )?;

            let group_properties_config = if let Some(properties) = &group.config {
                let properties_config: GroupConfig = properties.clone();
                properties_config
            } else {
                GroupConfig::default()
            };

            let dbt_group = DbtGroup {
                __common_attr__: CommonAttributes {
                    name: group_name.to_string(),
                    package_name: package_name.to_string(),
                    path: mpe.relative_path.clone(),
                    name_span: dbt_common::Span::from_serde_span(
                        mpe.name_span.clone(),
                        mpe.relative_path.clone(),
                    ),
                    original_file_path: mpe.relative_path.clone(),
                    unique_id: unique_id.clone(),
                    fqn: vec![],
                    description: Some(group.description.unwrap_or_default()),
                    patch_path: None,
                    checksum: Default::default(),
                    language: None,
                    raw_code: None,
                    tags: vec![],
                    meta: group_properties_config.meta.clone().unwrap_or_default(),
                },
                __base_attr__: NodeBaseAttributes {
                    database: "".to_string(),
                    schema: "".to_string(),
                    alias: "".to_string(),
                    relation_name: None,
                    quoting: Default::default(),
                    materialized: Default::default(),
                    static_analysis: Default::default(),
                    enabled: true,
                    extended_model: false,
                    persist_docs: None,
                    columns: BTreeMap::new(),
                    depends_on: NodeDependsOn::default(),
                    quoting_ignore_case: false,
                    refs: vec![],
                    sources: vec![],
                    metrics: vec![],
                },
                __group_attr__: DbtGroupAttr { owner: group.owner },
            };
            groups.insert(unique_id, Arc::new(dbt_group));
        }
    }
    Ok((groups, HashMap::new()))
}
