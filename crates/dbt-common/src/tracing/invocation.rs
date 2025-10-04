use dbt_telemetry::{Invocation, InvocationEvalArgs, create_process_event_data};

use crate::io_args::EvalArgs;

fn create_invocation_eval_args(eval_arg: &EvalArgs) -> InvocationEvalArgs {
    InvocationEvalArgs {
        command: eval_arg.command.clone(),
        profiles_dir: eval_arg
            .profiles_dir
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        packages_install_path: eval_arg
            .packages_install_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        target: eval_arg.target.clone(),
        profile: eval_arg.profile.clone(),
        vars: serde_json::to_string(&eval_arg.vars).expect("Failed to serialize vars"),
        limit: eval_arg.limit.map(|l| l as u64),
        num_threads: eval_arg.num_threads.map(|l| l as u64),
        selector: eval_arg.selector.clone(),
        select: eval_arg.select.iter().map(|s| s.to_string()).collect(),
        exclude: eval_arg.exclude.iter().map(|s| s.to_string()).collect(),
        indirect_selection: eval_arg.indirect_selection.map(|s| s.to_string()),
        output_keys: eval_arg.output_keys.iter().map(|s| s.to_string()).collect(),
        resource_types: eval_arg
            .resource_types
            .iter()
            .map(|s| s.to_string())
            .collect(),
        exclude_resource_types: eval_arg
            .exclude_resource_types
            .iter()
            .map(|s| s.to_string())
            .collect(),
        debug: Some(eval_arg.debug),
        log_format: Some(eval_arg.log_format.to_string()),
        log_level: eval_arg.log_level.map(|l| l.to_string()),
        log_path: eval_arg
            .log_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        target_path: eval_arg
            .target_path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        project_dir: eval_arg
            .project_dir
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        quiet: Some(eval_arg.quiet),
        write_json: Some(eval_arg.write_json),
        write_catalog: Some(eval_arg.write_catalog),
    }
}

/// Creates telemetry attributes for the Invocation span by extracting environment variables,
/// CLI flags, and other relevant information.
pub fn create_invocation_attributes(package: &str, eval_arg: &EvalArgs) -> Invocation {
    // Capture raw command string
    let raw_command = std::env::args().collect::<Vec<_>>().join(" ");

    Invocation {
        invocation_id: eval_arg.io.invocation_id.to_string(),
        raw_command,
        eval_args: Some(create_invocation_eval_args(eval_arg)),
        process_info: Some(create_process_event_data(package)),
        metrics: Default::default(),
    }
}
