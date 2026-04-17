//! DAG execution orchestrator.
//!
//! The runner owns the scheduling loop: it computes dispatch order, tracks
//! per-task state, and dispatches independent tasks in parallel via async tasks.
//! Task execution itself is delegated entirely to the `Executor`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use opentelemetry::{KeyValue, global};
use serde_json::Value;
use thiserror::Error;

use crate::dag::{DagDef, NodeId, Trigger};
use crate::executor::{DispatchPayload, Executor, ExecutorError, TaskOutcome};
use crate::operators::Operator;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Input to a DAG run.
pub struct RunConfig {
    pub dag: DagDef,
    pub executor: Arc<dyn Executor>,
}

/// The result of a completed DAG run.
#[derive(Debug)]
pub struct RunOutcome {
    pub run_id: String,
    pub dag_id: String,
    /// Outcomes for every task that was dispatched (succeeded or failed).
    pub task_outcomes: HashMap<NodeId, TaskOutcome>,
    /// Tasks that completed successfully, in completion order.
    pub succeeded: Vec<NodeId>,
}

#[derive(Debug, Error)]
pub enum RunError {
    #[error("task '{node_id}' failed: {source}")]
    TaskFailed {
        node_id: NodeId,
        #[source]
        source: ExecutorError,
    },

    #[error(
        "cannot dispatch '{node_id}': predecessor outputs contain duplicate output name '{output_name}'"
    )]
    DuplicateOutput {
        node_id: NodeId,
        output_name: String,
    },
}

// ---------------------------------------------------------------------------
// run()
// ---------------------------------------------------------------------------

/// Run a DAG to completion.
///
/// DagDef is trusted by construction; validation and resolution happened at
/// compile time. Dispatches tasks as their dependencies are satisfied, running
/// independent branches in parallel. Aborts on the first task failure for now.
#[tracing::instrument(
    skip(config),
    fields(
        dag.id         = %config.dag.dag_id,
        dag.pipeline_id = %config.dag.pipeline_id,
        dag.version    = %config.dag.version_hash,
        dag.trigger    = %trigger_type_str(&config.dag.trigger),
    )
)]
pub async fn run(config: RunConfig) -> Result<RunOutcome, RunError> {
    let start = Instant::now();
    let run_id = make_run_id();
    tracing::Span::current().record("run.id", run_id.as_str());

    let result = run_inner(run_id, config).await;

    let (status, dag_id) = match &result {
        Ok(o) => ("success", o.dag_id.clone()),
        Err(_) => ("failure", "<unknown>".to_string()),
    };

    global::meter("tinydag")
        .f64_histogram("tinydag.run.duration")
        .with_unit("s")
        .with_description("Duration of a complete DAG run")
        .build()
        .record(
            start.elapsed().as_secs_f64(),
            &[
                KeyValue::new("dag.id", dag_id),
                KeyValue::new("result", status),
            ],
        );

    result
}

async fn run_inner(run_id: String, config: RunConfig) -> Result<RunOutcome, RunError> {
    let dag = &config.dag;

    let trigger = trigger_type_str(&dag.trigger).to_string();

    let mut in_degree: HashMap<&str, usize> =
        dag.nodes.iter().map(|n| (n.id.as_str(), 0)).collect();
    let mut successors: HashMap<&str, Vec<&str>> =
        dag.nodes.iter().map(|n| (n.id.as_str(), vec![])).collect();

    for edge in &dag.edges {
        *in_degree.get_mut(edge.to.as_str()).unwrap() += 1;
        successors
            .get_mut(edge.from.as_str())
            .unwrap()
            .push(edge.to.as_str());
    }

    // Seed with nodes that have no dependencies.
    let mut ready: Vec<&str> = in_degree
        .iter()
        .filter(|&(_, &d)| d == 0)
        .map(|(id, _)| *id)
        .collect();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<(NodeId, Result<TaskOutcome, ExecutorError>)>(
        dag.nodes.len().max(1),
    );
    let mut in_flight: usize = 0;
    let mut task_outcomes: HashMap<NodeId, TaskOutcome> = HashMap::new();
    let mut succeeded: Vec<NodeId> = Vec::new();
    // Accumulated outputs from all completed nodes, used to resolve inputs.
    let mut outputs: HashMap<NodeId, HashMap<String, Value>> = HashMap::new();

    let tasks_succeeded_counter = global::meter("tinydag")
        .u64_counter("tinydag.run.tasks_succeeded")
        .with_description("Number of tasks that succeeded in a DAG run")
        .build();
    let tasks_failed_counter = global::meter("tinydag")
        .u64_counter("tinydag.run.tasks_failed")
        .with_description("Number of tasks that failed in a DAG run")
        .build();
    let run_attrs = [KeyValue::new("dag.id", dag.dag_id.clone())];

    loop {
        // Dispatch all currently-ready nodes.
        for node_id in ready.drain(..) {
            let node = dag.nodes.iter().find(|n| n.id == node_id).unwrap().clone();

            // Gather inputs: merge outputs from all predecessor nodes.
            // Duplicate output names across predecessors are a hard error.
            let mut inputs: HashMap<String, Value> = HashMap::new();
            for edge in dag.edges.iter().filter(|e| e.to == node_id) {
                for (output_name, value) in
                    outputs.get(edge.from.as_str()).cloned().unwrap_or_default()
                {
                    if inputs.contains_key(&output_name) {
                        // Drain in-flight before returning so tasks don't leak.
                        for _ in 0..in_flight {
                            let _ = rx.recv().await;
                        }
                        return Err(RunError::DuplicateOutput {
                            node_id: node_id.to_string(),
                            output_name,
                        });
                    }
                    inputs.insert(output_name, value);
                }
            }

            let payload = DispatchPayload::from_node(
                &node,
                run_id.clone(),
                dag.dag_id.clone(),
                dag.pipeline_id.clone(),
                dag.version_hash.clone(),
                dag.team.clone(),
                dag.user.clone(),
                trigger.clone(),
                inputs,
                dag.params.clone(),
            );

            let executor = Arc::clone(&config.executor);
            let tx2 = tx.clone();
            let owned_id = node_id.to_string();

            tracing::info!(
                run.id  = %run_id,
                node.id = %node_id,
                operator.type = %&node.task_ref.type_name(),
                "dispatching task"
            );

            tokio::task::spawn(async move {
                let result = executor.dispatch(payload).await;
                let _ = tx2.send((owned_id, result)).await;
            });

            in_flight += 1;
        }

        if in_flight == 0 {
            break;
        }

        // Wait for the next task to complete.
        let (node_id, result) = rx.recv().await.expect("runner channel closed unexpectedly");
        in_flight -= 1;

        match result {
            Err(error) => {
                tracing::error!(
                    run.id  = %run_id,
                    node.id = %node_id,
                    error   = %error,
                    "task failed"
                );
                tasks_failed_counter.add(1, &run_attrs);

                // Drain remaining in-flight completions before returning.
                for _ in 0..in_flight {
                    let _ = rx.recv().await;
                }

                return Err(RunError::TaskFailed {
                    node_id,
                    source: error,
                });
            }

            Ok(outcome) => {
                tracing::info!(
                    run.id  = %run_id,
                    node.id = %node_id,
                    "task succeeded"
                );
                tasks_succeeded_counter.add(1, &run_attrs);

                for (output_name, output_value) in &outcome.outputs {
                    tracing::info!(
                        task.id      = %node_id,
                        output.name  = %output_name,
                        output.value = %output_value,
                        "task output"
                    );
                }

                outputs.insert(node_id.clone(), outcome.outputs.clone());
                succeeded.push(node_id.clone());
                task_outcomes.insert(node_id.clone(), outcome);

                // Unlock successors whose in-degree drops to zero.
                let succs: Vec<&str> = successors
                    .get(node_id.as_str())
                    .cloned()
                    .unwrap_or_default();
                for succ in succs {
                    let deg = in_degree.get_mut(succ).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        ready.push(succ);
                    }
                }
            }
        }
    }

    Ok(RunOutcome {
        run_id,
        dag_id: dag.dag_id.clone(),
        task_outcomes,
        succeeded,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn trigger_type_str(trigger: &Trigger) -> &'static str {
    match trigger {
        Trigger::Manual => "manual",
        Trigger::Cron { .. } => "cron",
        Trigger::Event { .. } => "event",
        Trigger::PipelineCompletion { .. } => "pipeline_completion",
    }
}

fn make_run_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagDef;
    use crate::executor::LocalExecutor;

    async fn bash_executor() -> Arc<dyn Executor> {
        let test_bin = std::env::current_exe().expect("can't locate test binary");
        let binary = test_bin
            .parent()
            .unwrap() // …/deps
            .parent()
            .unwrap() // …/debug
            .join("tinydag-op-bash");
        let mut registry = HashMap::new();
        registry.insert("bash".to_string(), binary.to_string_lossy().into_owned());
        Arc::new(LocalExecutor::with_registry(registry).await)
    }

    fn compile_dag(src: &str) -> DagDef {
        crate::compiler::compile_source("test.star", src, None).unwrap()
    }

    #[tokio::test]
    async fn single_successful_task() {
        let dag = compile_dag(
            r#"
dag("test")
bash_operator("a", cmd="true")
"#,
        );
        let outcome = run(RunConfig {
            dag,
            executor: bash_executor().await,
        })
        .await
        .unwrap();
        assert_eq!(outcome.succeeded, vec!["a"]);
    }

    #[tokio::test]
    async fn failing_task_returns_run_error() {
        let dag = compile_dag(
            r#"
dag("test")
bash_operator("a", cmd="exit 1")
"#,
        );
        let err = run(RunConfig {
            dag,
            executor: bash_executor().await,
        })
        .await
        .unwrap_err();
        assert!(matches!(err, RunError::TaskFailed { node_id, .. } if node_id == "a"));
    }

    #[tokio::test]
    async fn linear_chain_runs_in_order() {
        let dag = compile_dag(
            r#"
dag("chain")
a = bash_operator("a", cmd="true")
b = bash_operator("b", cmd="true", depends_on=a)
bash_operator("c", cmd="true", depends_on=b)
"#,
        );
        let outcome = run(RunConfig {
            dag,
            executor: bash_executor().await,
        })
        .await
        .unwrap();
        assert_eq!(outcome.succeeded.len(), 3);
        let pos: HashMap<&str, usize> = outcome
            .succeeded
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();
        assert!(pos["a"] < pos["b"]);
        assert!(pos["b"] < pos["c"]);
    }

    #[tokio::test]
    async fn diamond_dag_all_nodes_complete() {
        // a -> b, a -> c, b -> d, c -> d
        let dag = compile_dag(
            r#"
dag("diamond")
a = bash_operator("a", cmd="true")
b = bash_operator("b", cmd="true", depends_on=a)
c = bash_operator("c", cmd="true", depends_on=a)
bash_operator("d", cmd="true", depends_on=[b, c])
"#,
        );
        let outcome = run(RunConfig {
            dag,
            executor: bash_executor().await,
        })
        .await
        .unwrap();
        assert_eq!(outcome.succeeded.len(), 4);
        let pos: HashMap<&str, usize> = outcome
            .succeeded
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();
        assert!(pos["a"] < pos["b"]);
        assert!(pos["a"] < pos["c"]);
        assert!(pos["b"] < pos["d"]);
        assert!(pos["c"] < pos["d"]);
    }

    #[tokio::test]
    async fn duplicate_output_name_is_an_error() {
        // a and b both produce name "x"; c depends on both — should fail.
        let dag = compile_dag(
            r#"
dag("dup")
a = bash_operator("a", cmd="printf '{\"outputs\":{\"x\":1}}' > tinydag_outputs.json")
b = bash_operator("b", cmd="printf '{\"outputs\":{\"x\":2}}' > tinydag_outputs.json")
bash_operator("c", cmd="true", depends_on=[a, b])
"#,
        );
        let err = run(RunConfig {
            dag,
            executor: bash_executor().await,
        })
        .await
        .unwrap_err();
        assert!(
            matches!(&err, RunError::DuplicateOutput { node_id, output_name }
                if node_id == "c" && output_name == "x"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn distinct_output_names_from_multiple_predecessors_merge_cleanly() {
        // a produces "x", b produces "y"; c depends on both — no collision.
        let dag = compile_dag(
            r#"
dag("merge")
a = bash_operator("a", cmd="printf '{\"outputs\":{\"x\":1}}' > tinydag_outputs.json")
b = bash_operator("b", cmd="printf '{\"outputs\":{\"y\":2}}' > tinydag_outputs.json")
bash_operator("c", cmd="true", depends_on=[a, b])
"#,
        );
        let outcome = run(RunConfig {
            dag,
            executor: bash_executor().await,
        })
        .await
        .unwrap();
        assert_eq!(outcome.succeeded.len(), 3);
    }
}
