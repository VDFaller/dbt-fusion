use dbt_schemas::schemas::{
    manifest::{
        DbtManifestV12, DbtNode
    }
};
use dbt_lint::{get_manifest};
use std::{env, path::Path};


fn inherit_column_descriptions<'a>(manifest: &'a mut DbtManifestV12, node_id: &'a str, col_name: &'a str) -> Result<(), String> {
    // This function will inherit column descriptions from the upstream model or source
    // todo: add sources, seeds, snapshots
    // mark unsafe if multiple upstream models have same column name
    //    or even better, know which upstream model to inherit from (SDF style)
    //    could possibly use the cached target/db/dbt/information_schema/output.parquet.  Not sure what would be faster. 

    if let Some(desc) = get_upstream_col_desc(manifest, node_id, col_name) {
        if let Some(DbtNode::Model(model)) = manifest.nodes.get_mut(node_id) {
            if let Some(col) = model.__base_attr__.columns.get_mut(col_name) {
                col.description = Some(desc);
                return Ok(());
            } else {
                return Err(format!("Column {} not found in model {}", col_name, node_id));
            }
        } else {
            return Err(format!("Node with id {} is not a model", node_id));
        }
    } else {
        return Err(format!("No upstream description found for column {} in node {}", col_name, node_id));
    }
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
            .filter_map(|dep_col| dep_col.description.as_ref().cloned())
            .next();
        return desc;
    }
    else {
        return None;
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let manifest_path = Path::new(&args[1]);
    let mut manifest= get_manifest(manifest_path);


    let node_id: &str = "model.dsa_dbt.stg_agile__multilevelbom";
    let col_name: &str = "fg_assembly_number";
    let _ = inherit_column_descriptions(&mut manifest, node_id, col_name);
    let col = manifest.nodes.get(node_id)
        .and_then(|node| match node {
            DbtNode::Model(model) => model.__base_attr__.columns.get(col_name),
            _ => None,
        })
        .cloned()
        .expect("Column not found in model");
    println!(
        "Inherited column description: {:?}",
        col.description.unwrap_or(String::from("<none>"))
        );

}
