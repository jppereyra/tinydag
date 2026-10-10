//! Compiles, registers and optionally runs a batch of pipelines for `tinydag add`.
//!
//! Progress is reported as [`AddEvent`]s over a channel so the front end that
//! displays it (plain text today) is decoupled from the work itself.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tinydag::compiler::CompileError;
use tinydag::dag::DagDef;
use tinydag::runner::RunError;
use tinydag::scheduler::Scheduler;
use tokio::sync::{Semaphore, mpsc::UnboundedSender};
use tokio::task::JoinSet;

/// Cap on pipelines compiled at once. Each compile runs on a blocking thread
/// and may hold script files and validation subprocess pipes open.
const MAX_CONCURRENT_COMPILES: usize = 512;

pub const ADD_USAGE: &str =
    "usage: tinydag add <pipeline.star> [<pipeline.star> ...] [--file <list>] [--run-now]";

pub struct AddArgs {
    pub paths: Vec<String>,
    pub run_now: bool,
}

/// Parses `tinydag add` arguments (`args[0]` is the binary, `args[1]` is `add`).
///
/// `--file <list>` reads one pipeline path per line; blank lines and lines
/// starting with `#` are skipped.
pub fn parse_add_args(args: &[String]) -> Result<AddArgs, String> {
    let mut paths = Vec::new();
    let mut run_now = false;
    let mut iter = args.iter().skip(2);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--run-now" => run_now = true,
            "--file" => {
                let list = iter
                    .next()
                    .ok_or_else(|| "error: --file requires a path argument".to_string())?;
                let content = std::fs::read_to_string(list)
                    .map_err(|e| format!("error: cannot read '{list}': {e}"))?;
                paths.extend(
                    content
                        .lines()
                        .map(str::trim)
                        .filter(|l| !l.is_empty() && !l.starts_with('#'))
                        .map(String::from),
                );
            }
            _ => paths.push(arg.clone()),
        }
    }
    if paths.is_empty() {
        return Err(ADD_USAGE.to_string());
    }
    Ok(AddArgs { paths, run_now })
}

/// Progress of one pipeline, identified by its index in the input path list.
#[derive(Clone, Debug)]
pub enum AddEvent {
    Compiling {
        idx: usize,
    },
    CompileFailed {
        idx: usize,
        errors: Vec<String>,
    },
    Registered {
        idx: usize,
        dag_id: String,
        node_count: usize,
    },
    Running {
        idx: usize,
    },
    Succeeded {
        idx: usize,
        run_id: String,
        task_count: usize,
    },
    RunFailed {
        idx: usize,
        error: String,
    },
}

impl AddEvent {
    /// Index of the pipeline this event is about.
    pub fn idx(&self) -> usize {
        match self {
            AddEvent::Compiling { idx }
            | AddEvent::CompileFailed { idx, .. }
            | AddEvent::Registered { idx, .. }
            | AddEvent::Running { idx }
            | AddEvent::Succeeded { idx, .. }
            | AddEvent::RunFailed { idx, .. } => *idx,
        }
    }
}

#[derive(Default)]
pub struct BatchSummary {
    pub compile_failed: usize,
    pub run_failed: usize,
}

impl BatchSummary {
    /// 3 if any run failed, else 1 if any pipeline failed to compile, else 0.
    pub fn exit_code(&self) -> i32 {
        if self.run_failed > 0 {
            3
        } else if self.compile_failed > 0 {
            1
        } else {
            0
        }
    }
}

/// Compiles every path in parallel, registering each pipeline as it compiles.
/// With `run_now`, then triggers every registered pipeline and waits for all runs.
pub async fn run_batch(
    paths: Vec<String>,
    run_now: bool,
    scheduler: &Scheduler,
    events: UnboundedSender<AddEvent>,
) -> BatchSummary {
    let mut summary = BatchSummary::default();
    // A closed receiver just means nobody is displaying progress.
    let emit = |ev| {
        let _ = events.send(ev);
    };

    let names = paths.clone();
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_COMPILES));
    let mut compiles = JoinSet::new();
    for (idx, path) in paths.into_iter().enumerate() {
        let semaphore = Arc::clone(&semaphore);
        let events = events.clone();
        compiles.spawn(async move {
            let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
            let _ = events.send(AddEvent::Compiling { idx });
            let result = tokio::task::spawn_blocking(move || compile_file(&path))
                .await
                .unwrap_or_else(|e| Err(vec![format!("compiler panicked: {e}")]));
            (idx, result)
        });
    }

    let mut registered: Vec<(usize, String)> = Vec::new();
    let mut registered_by: HashMap<String, usize> = HashMap::new();
    while let Some(joined) = compiles.join_next().await {
        let (idx, result) = joined.expect("compile task panicked");
        match result {
            Ok(dag) => {
                let dag_id = dag.dag_id().to_string();
                let node_count = dag.nodes().len();
                if scheduler.register(dag).is_err() {
                    let first = registered_by
                        .get(&dag_id)
                        .map_or("an earlier pipeline", |&i| names[i].as_str());
                    summary.compile_failed += 1;
                    emit(AddEvent::CompileFailed {
                        idx,
                        errors: vec![format!(
                            "dag_id '{dag_id}' is already registered by '{first}'"
                        )],
                    });
                    continue;
                }
                registered_by.insert(dag_id.clone(), idx);
                registered.push((idx, dag_id.clone()));
                emit(AddEvent::Registered {
                    idx,
                    dag_id,
                    node_count,
                });
            }
            Err(errors) => {
                summary.compile_failed += 1;
                emit(AddEvent::CompileFailed { idx, errors });
            }
        }
    }

    if !run_now {
        return summary;
    }

    let mut runs = JoinSet::new();
    for (idx, dag_id) in registered {
        let handle = scheduler
            .trigger(&dag_id)
            .expect("dag was registered above");
        emit(AddEvent::Running { idx });
        runs.spawn(async move { (idx, handle.await.expect("run task panicked")) });
    }

    while let Some(joined) = runs.join_next().await {
        let (idx, result) = joined.expect("run task panicked");
        match result {
            Ok(outcome) => emit(AddEvent::Succeeded {
                idx,
                run_id: outcome.run_id,
                task_count: outcome.succeeded.len(),
            }),
            Err(e) => {
                summary.run_failed += 1;
                let error = match e {
                    RunError::TaskFailed { .. } | RunError::TaskPanicked { .. } => e.to_string(),
                    RunError::DuplicateOutput {
                        node_id,
                        output_name,
                    } => format!(
                        "predecessors of '{node_id}' produce duplicate output name '{output_name}'"
                    ),
                };
                emit(AddEvent::RunFailed { idx, error });
            }
        }
    }

    summary
}

/// Reads and compiles one pipeline file, flattening errors to messages.
fn compile_file(path: &str) -> Result<DagDef, Vec<String>> {
    let star_path = Path::new(path);
    let source = std::fs::read_to_string(star_path)
        .map_err(|e| vec![format!("cannot read '{path}': {e}")])?;
    tinydag::compiler::compile(path, &source, star_path.parent()).map_err(|e| match e {
        CompileError::Eval(e) => vec![e.to_string()],
        CompileError::Validation(errs) => errs.iter().map(ToString::to_string).collect(),
    })
}

/// Prints an event as plain text: results to stdout, failures to stderr.
pub fn print_event(paths: &[String], event: &AddEvent) {
    let path = &paths[event.idx()];
    match event {
        AddEvent::Compiling { .. } | AddEvent::Running { .. } => {}
        AddEvent::CompileFailed { errors, .. } => {
            for e in errors {
                eprintln!("error in '{path}': {e}");
            }
        }
        AddEvent::Registered {
            dag_id, node_count, ..
        } => println!("registered {dag_id} ({node_count} nodes)"),
        AddEvent::Succeeded {
            run_id, task_count, ..
        } => println!("run {run_id} succeeded ({task_count} tasks)"),
        AddEvent::RunFailed { error, .. } => eprintln!("error in '{path}': {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(rest: &[&str]) -> Vec<String> {
        ["tinydag", "add"]
            .iter()
            .chain(rest)
            .map(|s| s.to_string())
            .collect()
    }

    #[tokio::test]
    async fn duplicate_dag_id_in_a_batch_is_reported_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let src = r#"
cfg = config(name="same")
a = bash_operator("a", cmd="true", inputs=[], outputs=[])
build(cfg, a)
"#;
        let paths: Vec<String> = ["one.star", "two.star"]
            .iter()
            .map(|name| {
                let p = dir.path().join(name);
                std::fs::write(&p, src).unwrap();
                p.to_string_lossy().into_owned()
            })
            .collect();

        let scheduler = Scheduler::new(Arc::new(tinydag::executor::LocalExecutor::new().await));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let summary = run_batch(paths.clone(), false, &scheduler, tx).await;

        let mut registered = 0;
        let mut failures = Vec::new();
        while let Some(event) = rx.recv().await {
            match event {
                AddEvent::Registered { .. } => registered += 1,
                AddEvent::CompileFailed { errors, .. } => failures.extend(errors),
                _ => {}
            }
        }
        assert_eq!(registered, 1);
        assert_eq!(summary.compile_failed, 1);
        assert_eq!(failures.len(), 1);
        assert!(
            failures[0].contains("dag_id 'same' is already registered by"),
            "{failures:?}"
        );
    }

    #[test]
    fn parses_positional_paths_and_run_now() {
        let a = parse_add_args(&args(&["a.star", "--run-now", "b.star"])).unwrap();
        assert_eq!(a.paths, ["a.star", "b.star"]);
        assert!(a.run_now);
    }

    #[test]
    fn file_list_skips_comments_and_blank_lines() {
        let list = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(list.path(), "# header\nx.star\n\n  y.star  \n").unwrap();
        let a = parse_add_args(&args(&["--file", &list.path().to_string_lossy()])).unwrap();
        assert_eq!(a.paths, ["x.star", "y.star"]);
        assert!(!a.run_now);
    }

    #[test]
    fn no_paths_is_a_usage_error() {
        assert!(parse_add_args(&args(&["--run-now"])).is_err());
        assert!(parse_add_args(&args(&["--file"])).is_err());
    }

    #[test]
    fn exit_code_prefers_run_failures() {
        let s = |compile_failed, run_failed| BatchSummary {
            compile_failed,
            run_failed,
        };
        assert_eq!(s(0, 0).exit_code(), 0);
        assert_eq!(s(2, 0).exit_code(), 1);
        assert_eq!(s(2, 1).exit_code(), 3);
    }
}
