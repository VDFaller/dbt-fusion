use dbt_schemas::schemas::{
    manifest::{
        DbtManifestV12, DbtNode
    }
};
use clap::Parser;
use dbt_lint::{check_all};
use dbt_serde_yaml::Value as YmlValue;
use std::sync::Arc;
use serde;

use dbt_common::{
    cancellation::CancellationTokenSource,
    FsResult,
};
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_loader::{args::LoadArgs, load};
use dbt_parser::{args::ResolveArgs, resolver::resolve};
use dbt_schemas::{
    schemas::{Nodes, manifest::build_manifest},
    state::Macros,
};
use dbt_sa_lib::dbt_sa_clap::{Cli, from_main};


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

#[tokio::main]
async fn main() -> FsResult<()> {
    let cli = Cli::parse();
    let system_args = from_main(&cli);

    let eval_args = cli.to_eval_args(system_args)?;
    let invocation_id = eval_args.io.invocation_id.to_string();

    let load_args = LoadArgs::from_eval_args(&eval_args);
    let invocation_args = InvocationArgs::from_eval_args(&eval_args);
    let _cts = CancellationTokenSource::new();
    let token = _cts.token();

    let (dbt_state, threads, _) = load(&load_args, &invocation_args, &token).await?;

    let eval_args = eval_args
        .with_target(dbt_state.dbt_profile.target.to_string())
        .with_threads(threads);

    let resolve_args = ResolveArgs::try_from_eval_args(&eval_args)?;
    let invocation_args = InvocationArgs::from_eval_args(&eval_args);

    let (resolved_state, _jinja_env) = resolve(
        &resolve_args,
        &invocation_args,
        Arc::new(dbt_state),
        Macros::default(),
        Nodes::default(),
        None,   // omit the optional event listener for the simplest case
        &token,
    )
    .await?;

    let dbt_manifest = build_manifest(&invocation_id, &resolved_state);

    let failures = check_all(&dbt_manifest);

    println!("Nodes without description: {:?}", failures.model_failures.no_descriptions.len());
    println!("Number of models without tags: {}", failures.model_failures.no_tags.len());
    println!("Models with columns missing descriptions: {:?}", failures.model_failures.column_failures.len());

    println!("Sources without description: {:?}", failures.source_failures.no_descriptions.len());
    Ok(())
}