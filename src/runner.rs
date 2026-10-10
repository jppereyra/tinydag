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
use crate::executor::{DispatchPayload, Executor, ExecutorError, RunContext, TaskOutcome};
use crate::operators::Operator;
use crate::plan::Plan;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Input to a DAG run.
pub struct RunConfig {
    pub dag: Arc<DagDef>,
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

    let status = if result.is_ok() { "success" } else { "failure" };

    // Metric attributes must have bounded cardinality: per-DAG and per-node
    // detail belongs on the spans, not here.

    global::meter("tinydag")
        .f64_histogram("tinydag.run.duration")
        .with_unit("s")
        .with_description("Duration of a complete DAG run")
        .build()
        .record(
            start.elapsed().as_secs_f64(),
            &[KeyValue::new("result", status)],
        );

    result
}

async fn run_inner(run_id: String, config: RunConfig) -> Result<RunOutcome, RunError> {
    let plan = Plan::new(config.dag);
    let dag = plan.dag();

    let ctx = RunContext {
        run_id: run_id.clone(),
        dag_id: dag.dag_id.clone(),
        pipeline_id: dag.pipeline_id.clone(),
        dag_version: dag.version_hash.clone(),
        team: dag.team.clone(),
        user: dag.user.clone(),
        trigger_type: trigger_type_str(&dag.trigger).to_string(),
    };

    // Remaining unfinished predecessors per node; a node is ready at zero.
    let mut deps_left: Vec<u32> = plan.in_degree().to_vec();
    let mut ready: Vec<u32> = plan.roots().to_vec();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<(u32, Result<TaskOutcome, ExecutorError>)>(
        dag.nodes.len().max(1),
    );
    let mut in_flight: usize = 0;
    let mut task_outcomes: HashMap<NodeId, TaskOutcome> = HashMap::new();
    let mut succeeded: Vec<NodeId> = Vec::new();
    // Outputs of completed nodes, by node index, used to resolve inputs.
    let mut outputs: Vec<Option<HashMap<String, Value>>> = vec![None; dag.nodes.len()];

    let tasks_succeeded_counter = global::meter("tinydag")
        .u64_counter("tinydag.run.tasks_succeeded")
        .with_description("Number of tasks that succeeded in a DAG run")
        .build();
    let tasks_failed_counter = global::meter("tinydag")
        .u64_counter("tinydag.run.tasks_failed")
        .with_description("Number of tasks that failed in a DAG run")
        .build();

    loop {
        // Dispatch all currently-ready nodes.
        for ix in ready.drain(..) {
            let node = plan.node(ix);

            // Gather inputs: forward only outputs that the node declared as inputs.
            let mut inputs: HashMap<String, Value> = HashMap::new();
            for &pred in plan.predecessors(ix) {
                let Some(pred_outputs) = &outputs[pred as usize] else {
                    continue;
                };
                for (output_name, value) in pred_outputs {
                    if !node.inputs.iter().any(|s| s == output_name) {
                        continue;
                    }
                    if inputs.contains_key(output_name) {
                        for _ in 0..in_flight {
                            let _ = rx.recv().await;
                        }
                        return Err(RunError::DuplicateOutput {
                            node_id: node.id.clone(),
                            output_name: output_name.clone(),
                        });
                    }
                    inputs.insert(output_name.clone(), value.clone());
                }
            }

            let payload = DispatchPayload::from_node(node, ctx.clone(), inputs, dag.params.clone());

            let executor = Arc::clone(&config.executor);
            let tx2 = tx.clone();

            tracing::info!(
                run.id  = %run_id,
                node.id = %node.id,
                operator.type = %&node.task_ref.type_name(),
                "dispatching task"
            );

            tokio::task::spawn(async move {
                let result = executor.dispatch(payload).await;
                let _ = tx2.send((ix, result)).await;
            });

            in_flight += 1;
        }

        if in_flight == 0 {
            break;
        }

        // Wait for the next task to complete.
        let (ix, result) = rx.recv().await.expect("runner channel closed unexpectedly");
        in_flight -= 1;
        let node_id = &plan.node(ix).id;

        match result {
            Err(error) => {
                tracing::error!(
                    run.id  = %run_id,
                    node.id = %node_id,
                    error   = %error,
                    "task failed"
                );
                tasks_failed_counter.add(1, &[]);

                // Drain remaining in-flight completions before returning.
                for _ in 0..in_flight {
                    let _ = rx.recv().await;
                }

                return Err(RunError::TaskFailed {
                    node_id: node_id.clone(),
                    source: error,
                });
            }

            Ok(outcome) => {
                tracing::info!(
                    run.id  = %run_id,
                    node.id = %node_id,
                    "task succeeded"
                );
                tasks_succeeded_counter.add(1, &[]);

                for (output_name, output_value) in &outcome.outputs {
                    tracing::info!(
                        task.id      = %node_id,
                        output.name  = %output_name,
                        output.value = %output_value,
                        "task output"
                    );
                }

                outputs[ix as usize] = Some(outcome.outputs.clone());
                succeeded.push(node_id.clone());
                task_outcomes.insert(node_id.clone(), outcome);

                // Unlock successors whose last predecessor just finished.
                for &succ in plan.successors(ix) {
                    let left = &mut deps_left[succ as usize];
                    *left -= 1;
                    if *left == 0 {
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
    use crate::testing::{MockBehavior, MockExecutor};

    async fn bash_executor() -> Arc<dyn Executor> {
        let test_bin = std::env::current_exe().expect("can't locate test binary");
        let binary = test_bin
            .parent()
            .unwrap() // …/deps
            .parent()
            .unwrap() // …/debug
            .join("tinydag-op-bash");
        Arc::new(
            LocalExecutor::new()
                .await
                .with_op_binary("bash", binary.to_string_lossy()),
        )
    }

    fn compile_dag(src: &str) -> Arc<DagDef> {
        Arc::new(crate::compiler::compile("test.star", src, None).unwrap())
    }

    /// Attribute keys on every data point in `data`.
    fn attribute_keys<T>(
        data: &opentelemetry_sdk::metrics::data::MetricData<T>,
        keys: &mut std::collections::BTreeSet<String>,
    ) {
        use opentelemetry_sdk::metrics::data::MetricData;
        let mut add = |kvs: &mut dyn Iterator<Item = &KeyValue>| {
            keys.extend(kvs.map(|kv| kv.key.to_string()));
        };
        match data {
            MetricData::Gauge(g) => g.data_points().for_each(|p| add(&mut p.attributes())),
            MetricData::Sum(s) => s.data_points().for_each(|p| add(&mut p.attributes())),
            MetricData::Histogram(h) => h.data_points().for_each(|p| add(&mut p.attributes())),
            MetricData::ExponentialHistogram(h) => {
                h.data_points().for_each(|p| add(&mut p.attributes()))
            }
        }
    }

    #[tokio::test]
    async fn metrics_carry_no_per_dag_or_per_node_attributes() {
        use opentelemetry_sdk::metrics::data::AggregatedMetrics;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        };

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        global::set_meter_provider(provider.clone());

        let dag = compile_dag(
            r#"
cfg = config(name="metrics", pipeline_id="p")
a = bash_operator("a", cmd="true", inputs=[], outputs=[])
b = bash_operator("b", cmd="true", inputs=[], outputs=[], depends_on=a)
build(cfg, b)
"#,
        );
        run(RunConfig {
            dag,
            executor: bash_executor().await,
        })
        .await
        .unwrap();
        provider.force_flush().unwrap();

        // Each distinct value of these is its own time series; at thousands of
        // DAGs the SDK's cardinality limit silently folds them into overflow.
        let mut keys = std::collections::BTreeSet::new();
        for rm in exporter.get_finished_metrics().unwrap() {
            for metric in rm.scope_metrics().flat_map(|sm| sm.metrics()) {
                match metric.data() {
                    AggregatedMetrics::F64(d) => attribute_keys(d, &mut keys),
                    AggregatedMetrics::U64(d) => attribute_keys(d, &mut keys),
                    AggregatedMetrics::I64(d) => attribute_keys(d, &mut keys),
                }
            }
        }
        assert!(
            keys.contains("result"),
            "expected metrics, got keys {keys:?}"
        );
        for unbounded in ["dag.id", "node.id", "dag.pipeline_id"] {
            assert!(
                !keys.contains(unbounded),
                "metric attribute '{unbounded}' in {keys:?}"
            );
        }
    }

    #[tokio::test]
    async fn roots_dispatch_in_declaration_order() {
        let dag = compile_dag(
            r#"
cfg = config(name="fan-in")
roots = [bash_operator("r%d" % i, cmd="true", inputs=[], outputs=[]) for i in range(8)]
done = bash_operator("done", cmd="true", inputs=[], outputs=[], depends_on=roots)
build(cfg, done)
"#,
        );
        // Ready nodes used to come out of a HashMap, so this order varied run to run.
        for _ in 0..20 {
            let executor = Arc::new(MockExecutor::new());
            run(RunConfig {
                dag: Arc::clone(&dag),
                executor: executor.clone(),
            })
            .await
            .unwrap();
            assert_eq!(
                executor.dispatch_order(),
                ["r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7", "done"]
            );
        }
    }

    #[tokio::test]
    async fn failure_stops_dispatch_of_successors() {
        let dag = compile_dag(
            r#"
cfg = config(name="fails")
a = bash_operator("a", cmd="true", inputs=[], outputs=[])
b = bash_operator("b", cmd="true", inputs=[], outputs=[], depends_on=a)
build(cfg, b)
"#,
        );
        let executor = Arc::new(MockExecutor::new().with("a", MockBehavior::Fail { code: 3 }));
        let err = run(RunConfig {
            dag,
            executor: executor.clone(),
        })
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            RunError::TaskFailed { ref node_id, source: ExecutorError::TaskFailed { code: 3, .. } }
                if node_id == "a"
        ));
        assert_eq!(executor.dispatch_order(), ["a"]);
    }

    #[tokio::test]
    async fn only_declared_inputs_reach_a_task() {
        let dag = compile_dag(
            r#"
cfg = config(name="inputs")
a = bash_operator("a", cmd="true", inputs=[], outputs=["x", "y"])
b = bash_operator("b", cmd="true", inputs=["x"], outputs=[], depends_on=a)
build(cfg, b)
"#,
        );
        let a_outputs = HashMap::from([
            ("x".to_string(), serde_json::json!(1)),
            ("y".to_string(), serde_json::json!(2)),
        ]);
        let executor = Arc::new(MockExecutor::new().with("a", MockBehavior::Succeed(a_outputs)));
        run(RunConfig {
            dag,
            executor: executor.clone(),
        })
        .await
        .unwrap();

        let b = executor
            .dispatched()
            .into_iter()
            .find(|p| p.node_id == "b")
            .unwrap();
        assert_eq!(
            b.inputs,
            HashMap::from([("x".to_string(), serde_json::json!(1))])
        );
    }

    #[tokio::test]
    async fn single_successful_task() {
        let dag = compile_dag(
            r#"
cfg = config(name="test")
a = bash_operator("a", cmd="true", inputs=[], outputs=[])
build(cfg, a)
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
cfg = config(name="test")
a = bash_operator("a", cmd="exit 1", inputs=[], outputs=[])
build(cfg, a)
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
cfg = config(name="chain")
a = bash_operator("a", cmd="true", inputs=[], outputs=[])
b = bash_operator("b", cmd="true", inputs=[], outputs=[], depends_on=a)
c = bash_operator("c", cmd="true", inputs=[], outputs=[], depends_on=b)
build(cfg, c)
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
cfg = config(name="diamond")
a = bash_operator("a", cmd="true", inputs=[], outputs=[])
b = bash_operator("b", cmd="true", inputs=[], outputs=[], depends_on=a)
c = bash_operator("c", cmd="true", inputs=[], outputs=[], depends_on=a)
d = bash_operator("d", cmd="true", inputs=[], outputs=[], depends_on=[b, c])
build(cfg, d)
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

    #[test]
    fn duplicate_output_name_is_an_error() {
        // a and b both declare output "x"; c depends on both — caught at compile time.
        let result = crate::compiler::compile(
            "test.star",
            r#"
cfg = config(name="dup")
a = bash_operator("a",
    cmd="printf '{\"outputs\":{\"x\":1}}' > tinydag_outputs.json",
    inputs=[], outputs=["x"])
b = bash_operator("b",
    cmd="printf '{\"outputs\":{\"x\":2}}' > tinydag_outputs.json",
    inputs=[], outputs=["x"])
c = bash_operator("c", cmd="true", inputs=["x"], outputs=[], depends_on=[a, b])
build(cfg, c)
"#,
            None,
        );
        assert!(
            matches!(&result, Err(crate::compiler::CompileError::Validation(errs))
            if errs.iter().any(|e| matches!(e,
                crate::validation::ValidationError::FanInOutputConflict { node_id, output_name, .. }
                if node_id == "c" && output_name == "x"
            ))),
            "unexpected result: {result:?}"
        );
    }

    #[tokio::test]
    async fn distinct_output_names_from_multiple_predecessors_merge_cleanly() {
        // a produces "x", b produces "y"; c depends on both — no collision.
        let dag = compile_dag(
            r#"
cfg = config(name="merge")
a = bash_operator("a", cmd="printf '{\"outputs\":{\"x\":1}}' > tinydag_outputs.json", inputs=[], outputs=["x"])
b = bash_operator("b", cmd="printf '{\"outputs\":{\"y\":2}}' > tinydag_outputs.json", inputs=[], outputs=["y"])
c = bash_operator("c", cmd="true", inputs=["x", "y"], outputs=[], depends_on=[a, b])
build(cfg, c)
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
