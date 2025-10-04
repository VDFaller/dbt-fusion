use crate::task::utils::{maybe_normalize_tmp_paths, relative_to_git_root};

use super::{
    ProjectEnv, TestEnv,
    task_seq::CommandFn,
    utils::{
        maybe_normalize_schema_name, maybe_normalize_slashes, maybe_normalize_time,
        normalize_inline_sql_files, normalize_version,
    },
};
use futures::FutureExt as _;
use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use std::{
    env,
    fs::File,
    io::{Read as _, Write as _},
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    sync::Arc,
};

use dbt_test_primitives::is_update_golden_files_mode;

use dbt_common::{
    FsResult,
    stdfs::{self},
};

pub(super) type TextualPatch = String;

// Snowflake prompt for our REPL
static SNOWFLAKE_PROMPT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d+\(snowflake\[local\].*").unwrap());

fn postprocess_actual(content: String, sort_output: bool) -> String {
    let res = [
        filter_lines,
        maybe_normalize_schema_name,
        maybe_normalize_time,
        normalize_version,
        maybe_normalize_tmp_paths,
        normalize_inline_sql_files,
    ]
    .iter()
    .fold(content, |acc, transform| transform(acc));

    if sort_output { sort_lines(res) } else { res }
}

fn postprocess_golden(content: String, sort_output: bool) -> String {
    let res = [
        maybe_normalize_slashes,
        maybe_normalize_schema_name,
        maybe_normalize_time,
        normalize_version,
        maybe_normalize_tmp_paths,
        normalize_inline_sql_files,
    ]
    .iter()
    .fold(content, |acc, transform| transform(acc));

    if sort_output { sort_lines(res) } else { res }
}

pub(super) fn diff_goldie<P: Fn(String) -> String>(
    goldie_type: &str,
    postprocessed_actual: String,
    goldie_path: &Path,
    goldie_post_processor: P,
) -> Option<TextualPatch> {
    let goldie_exists = goldie_path.exists();
    let golden = if goldie_exists {
        stdfs::read_to_string(goldie_path).unwrap_or_else(|_| {
            panic!(
                "cannot read golden {} from {}",
                goldie_type,
                goldie_path.display()
            )
        })
    } else {
        "".to_string()
    };
    let golden = goldie_post_processor(golden);
    let actual = maybe_normalize_slashes(postprocessed_actual);

    if goldie_exists && golden == actual {
        return None;
    }

    let relative_golden_path =
        relative_to_git_root(goldie_path).unwrap_or_else(|| goldie_path.to_path_buf());
    let original_filename = if !goldie_exists {
        "/dev/null".to_string()
    } else {
        PathBuf::from("i")
            .join(&relative_golden_path)
            .to_string_lossy()
            .to_string()
    };
    let modified_filename = PathBuf::from("w")
        .join(&relative_golden_path)
        .to_string_lossy()
        .to_string();

    let patch = diffy::DiffOptions::new()
        .set_original_filename(original_filename)
        .set_modified_filename(modified_filename)
        .create_patch(&golden, &actual);

    Some(patch.to_string())
}

pub struct CompareEnv {
    pub project_dir: PathBuf,
    pub target_dir: PathBuf,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
    pub goldie_stdout_path: PathBuf,
    pub goldie_stderr_path: PathBuf,
}

pub fn create_compare_env(
    name: &str,
    project_env: &ProjectEnv,
    test_env: &TestEnv,
    task_index: usize,
) -> CompareEnv {
    // inputs are read from here
    let project_dir = &project_env.absolute_project_dir;
    // golden files are read from here
    let golden_dir = &test_env.golden_dir;
    // Target dir is in scratch space
    let target_dir = test_env.temp_dir.join("target");

    // Prepare stdout and stderr
    let task_suffix = if task_index > 0 {
        format!("_{task_index}")
    } else {
        "".to_string()
    };

    let stdout_path = test_env
        .temp_dir
        .join(format!("{name}{task_suffix}.stdout"));
    let stderr_path = test_env
        .temp_dir
        .join(format!("{name}{task_suffix}.stderr"));
    let goldie_stdout_path = golden_dir.join(format!("{name}{task_suffix}.stdout"));
    let goldie_stderr_path = golden_dir.join(format!("{name}{task_suffix}.stderr"));

    CompareEnv {
        project_dir: project_dir.clone(),
        target_dir,
        stdout_path,
        stderr_path,
        goldie_stdout_path,
        goldie_stderr_path,
    }
}

#[allow(clippy::too_many_arguments)]
/// Executes a command and compares its output to the golden files.
///
/// Returns:
///  - If the command ran successfully and the output matches the golden files,
///    returns an empty vector.
///  - If the command ran successfully but the output does not match the golden
///    files, returns a vector of printable patches for each non-matching file.
///  - If the command failed to run, returns an error.
pub async fn execute_and_compare(
    // name of the task used to create file names (this is usually test name)
    name: &str,
    // command to execute as a vector
    cmd_vec: &[String],
    project_env: &ProjectEnv,
    test_env: &TestEnv,
    task_index: usize,
    // the actual function that will execute the given command after
    // necessary/common preparation
    sort_output: bool,
    exe: Arc<CommandFn>,
) -> FsResult<Vec<TextualPatch>> {
    let compare_env = create_compare_env(name, project_env, test_env, task_index);

    let stdout_file = stdfs::File::create(&compare_env.stdout_path)?;
    let stderr_file = stdfs::File::create(&compare_env.stderr_path)?;

    let res = AssertUnwindSafe(exe(
        cmd_vec.to_vec(),
        compare_env.project_dir,
        compare_env.target_dir,
        stdout_file,
        stderr_file,
        test_env.get_tracing_handle(),
    ))
    .catch_unwind()
    .await;

    match res {
        Ok(Ok(_exit_code)) => compare_or_update(
            is_update_golden_files_mode(),
            sort_output,
            compare_env.stderr_path,
            compare_env.goldie_stderr_path,
            compare_env.stdout_path,
            compare_env.goldie_stdout_path,
        ),
        Ok(Err(e)) => {
            eprintln!("error executing command {cmd_vec:?}: {e}");
            // TODO: this is kept to preserve existing behavior, where this
            // error was silently ignored. We should probably
            // dump_file_to_stderr then propagate the error instead:
            compare_or_update(
                is_update_golden_files_mode(),
                sort_output,
                compare_env.stderr_path,
                compare_env.goldie_stderr_path,
                compare_env.stdout_path,
                compare_env.goldie_stdout_path,
            )
        }
        Err(payload) => {
            eprintln!("command {cmd_vec:?} panicked during execution");

            // Best effort attempt to dump the captured stderr:
            let _ = dump_file_to_stderr(&compare_env.stderr_path);

            std::panic::resume_unwind(payload);
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn compare_or_update(
    is_update: bool,
    sort_output: bool,
    stderr_path: PathBuf,
    goldie_stderr_path: PathBuf,
    stdout_path: PathBuf,
    goldie_stdout_path: PathBuf,
) -> FsResult<Vec<TextualPatch>> {
    let stdout_content = stdfs::read_to_string(&stdout_path)?;
    let stdout_content = postprocess_actual(stdout_content, sort_output);
    let stderr_content = stdfs::read_to_string(&stderr_path)?;
    let stderr_content = postprocess_actual(stderr_content, sort_output);

    if is_update {
        // Copy stdout and stderr to goldie_stdout and goldie_stderr Note: we
        // can't use move here because the source and target files may not be on
        // the same filesystem
        stdfs::write(&goldie_stdout_path, stdout_content)?;
        stdfs::write(&goldie_stderr_path, stderr_content)?;
        Ok(vec![])
    } else {
        // Compare the generated files to the golden files
        let patches = diff_goldie("stderr", stderr_content, &goldie_stderr_path, |golden| {
            postprocess_golden(golden, sort_output)
        })
        .into_iter()
        .chain(diff_goldie(
            "stdout",
            stdout_content,
            &goldie_stdout_path,
            |golden| postprocess_golden(golden, sort_output),
        ))
        .collect::<Vec<_>>();
        Ok(patches)
    }
}

fn sort_lines(content: String) -> String {
    content.lines().sorted().collect::<Vec<_>>().join("\n")
}

fn filter_lines_internal(content: String, in_emacs: bool) -> String {
    const KNOWN_NOISE: &[&str] = &[
        " has been running for over",
        "last updated",
        "Detected unsafe introspection which may lead to non-deterministic static analysis.",
        "New version available",
    ];

    let mut res = content
        .lines()
        .filter_map(|line| {
            if KNOWN_NOISE.iter().any(|noise| line.contains(noise)) || is_all_whitespace(line) {
                // Purge noise and blank lines
                None
            } else if in_emacs && SNOWFLAKE_PROMPT.is_match(line) {
                // In Emacs we need to filter our REPL prompt.
                Some("")
            } else {
                // For other lines, trim ending whitespaces to reduce false
                // negatives:
                Some(line.trim_end())
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    if content.ends_with('\n') {
        res.push('\n');
    }
    res
}

fn filter_lines(content: String) -> String {
    filter_lines_internal(content, env::var("INSIDE_EMACS").is_ok())
}

fn is_all_whitespace(s: &str) -> bool {
    s.chars().all(|c| c.is_whitespace())
}

fn dump_file_to_stderr(path: &Path) -> std::io::Result<()> {
    let mut file = File::open(path)?;
    let size = file.metadata().map(|m| m.len() as usize).ok();
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(size.unwrap_or(0))?;
    file.read_to_end(&mut buffer)?;

    std::io::stderr().write_all(&buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_lines() {
        let lines = filter_lines("abc \n has been running for over \n 123".to_string());
        assert_eq!("abc\n 123", lines);
    }

    #[test]
    fn test_filter_repl_prompt() {
        let lines = filter_lines_internal("abc \n0(snowflake[local])> \n 123".to_string(), true);
        assert_eq!("abc\n\n 123", lines);
    }

    #[test]
    fn test_normalize_time() {
        let line = " Succeeded [ 44.65s] test  fusion_tests_schema__ga_analytics_regression__alex.source_unique_incident_io_severity_id";
        let postprocess_actual = postprocess_actual(line.to_string(), true);
        assert_eq!(
            " Succeeded [duration] test  fusion_tests_schema__replaced.source_unique_incident_io_severity_id",
            postprocess_actual
        );
    }

    #[test]
    fn test_normalize_schema_case_insensitive() {
        let line = "FUSION_REGRESSION_TESTING_CLONE.FUSION_TESTS_SCHEMA__ALEX.SELF_SERVICE_ACCOUNTING_ACTIVITY_SNAPSHOT";
        let postprocess_actual = postprocess_actual(line.to_string(), false);
        assert_eq!(
            "FUSION_REGRESSION_TESTING_CLONE.fusion_tests_schema__replaced.SELF_SERVICE_ACCOUNTING_ACTIVITY_SNAPSHOT",
            postprocess_actual
        );
    }

    #[test]
    fn test_normalize_multi_unit_duration_phrase() {
        let line = "Finished 'run' target 'databricks' with 1 error in 4s 703ms 195us 939ns";
        let postprocess_actual = postprocess_actual(line.to_string(), false);
        assert_eq!(
            "Finished 'run' target 'databricks' with 1 error in duration",
            postprocess_actual
        );
    }

    #[test]
    fn test_normalize_multi_unit_duration_standalone() {
        let line = "32ms 101us 694ns";
        let postprocess_actual = postprocess_actual(line.to_string(), false);
        assert_eq!("duration", postprocess_actual);
    }

    #[test]
    fn test_normalize_inline_sql_files() {
        let line = "Compiling model inline_a1b2c3d4.sql to target/compiled/inline_a1b2c3d4.sql";
        let postprocess_actual = postprocess_actual(line.to_string(), false);
        assert_eq!(
            "Compiling model inline_#randhash#.sql to target/compiled/inline_#randhash#.sql",
            postprocess_actual
        );
    }
}
