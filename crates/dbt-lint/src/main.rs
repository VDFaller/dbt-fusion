use clap::Parser;
use dbt_lint::{check_all, osmosis::inherit_column_descriptions};
use std::sync::Arc;

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

    println!("Nodes without description before fix: {:?}", failures.model_failures.no_descriptions.len());
    println!("Number of models without tags: {}", failures.model_failures.no_tags.len());
    println!("Models with columns missing descriptions: {:?}", failures.model_failures.column_failures.len());

    println!("Sources without description: {:?}", failures.source_failures.no_descriptions.len());

    // let column_failures: Vec<(String, Vec<String>)> = failures
    //     .model_failures
    //     .column_failures
    //     .iter()
    //     .map(|cf| {
    //         (
    //             cf.model.to_string(),
    //             cf.no_descriptions
    //                 .iter()
    //                 .map(|col| col.to_string())
    //                 .collect(),
    //         )
    //     })
    //     .collect();

    // // drop(failures); // so it stops borrowing dbt_manifest

    // for column_failure in column_failures {
    //     let model_id = column_failure.0.as_str();
    //     for col_name in column_failure.1 {

    //         match inherit_column_descriptions(&mut dbt_manifest, model_id, col_name.as_str()) {
    //             Ok(_) => println!("Inherited description for column {} in model {}", col_name, model_id),
    //             Err(e) => println!("Failed to inherit description for column {} in model {}: {}", col_name, model_id, e),
    //         }
    //     }
    // }
    // let fixed_failures = check_all(&dbt_manifest);
    // println!("Nodes without description after fix: {:?}", fixed_failures.model_failures.no_descriptions.len());

    Ok(())
}