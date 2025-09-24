use dbt_schemas::schemas::{
    manifest::{
        DbtManifestV12, DbtNode
    }
};
use dbt_lint::{get_manifest};
use dbt_serde_yaml::{from_str, to_string};
use dbt_serde_yaml::Value as YmlValue;
use std::{fs, env, path::Path};
use std::sync::Arc;
use serde;


fn inherit_column_descriptions<'a>(manifest: &'a mut DbtManifestV12, node_id: &'a str, col_name: &'a str) -> Result<(), String> {
    // This function will inherit column descriptions from the upstream model or source
    // todo: add sources, seeds, snapshots
    // mark unsafe if multiple upstream models have same column name
    //    or even better, know which upstream model to inherit from (SDF style)
    //    could possibly use the cached target/db/dbt/information_schema/output.parquet.  Not sure what would be faster. 

    let desc = match get_upstream_col_desc(manifest, node_id, col_name) {
        Some(desc) => desc,
        None => return Err(format!("No upstream description found for column {} in node {}", col_name, node_id)),
    };
    let model = match manifest.nodes.get_mut(node_id) {
        Some(DbtNode::Model(model)) => model,
        Some(_) => return Err(format!("Node with id {} is not a model", node_id)),
        None => return Err(format!("Node with id {} not found", node_id)),
    };

    let col = match model.__base_attr__.columns.get_mut(col_name) {
        Some(col) => Arc::get_mut(col).expect("Failed to get mutable reference to column"),
        None => return Err(format!("Column {} not found in model {}", col_name, node_id)),
    };
    
    col.description = Some(desc);
    Ok(())
}

fn get_upstream_col_desc<'a>(
    manifest: &'a DbtManifestV12,
    node_id: &'a str,
    col_name: &'a str,
) -> Option<String> {
    if let Some(DbtNode::Model(model)) = manifest.nodes.get(node_id) {
        let desc = model
            .__base_attr__
            .depends_on
            .nodes
            .iter()
            .filter_map(|upstream_id| {
                // the upstream id can be a node or a source
                manifest.nodes.get(upstream_id)
                    .map(|upstream_node| match upstream_node {
                        DbtNode::Model(upstream_model) => upstream_model.__base_attr__.columns.get(col_name),
                        DbtNode::Seed(upstream_seed) => upstream_seed.__base_attr__.columns.get(col_name),
                        DbtNode::Snapshot(upstream_snapshot) => upstream_snapshot.__base_attr__.columns.get(col_name),
                        _ => None,
                    })
                    .flatten()
                    .or_else(|| {
                        manifest.sources.get(upstream_id)
                            .and_then(|source| source.columns.get(col_name))
                    })
            })
            .filter_map(|dep_col| (*dep_col).description.as_ref().cloned())
            .next();
        return desc;
    }
    else {
        return None;
    }
}


#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Model {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<YmlValue>>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct ModelsFile {
    version: u8,
    models: Vec<Model>,
}

fn main() {
    // Read the YAML file

    let yaml_str = fs::read_to_string("crates/dbt-lint/src/test.yml").expect("Failed to read test.yml");
    let mut data: ModelsFile = from_str(&yaml_str).expect("Failed to parse YAML");

    // Add a new model "c"
    data.models.push(Model {
        name: "c".to_string(),
        description: None,
        columns: None,
    });

    // Write to a new YAML file
    let out_str = to_string(&data).expect("Failed to serialize YAML");
    fs::write("crates/dbt-lint/src/test_out.yml", out_str).expect("Failed to write test_out.yml");
}
