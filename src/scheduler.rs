//! v0.1 manual-trigger scheduler.
//!
//! Holds a registry of compiled DAGs and dispatches on-demand runs via
//! `trigger()`. Each triggered run executes as an independent tokio task.

use std::sync::Arc;

use dashmap::DashMap;

use crate::dag::DagDef;
use crate::executor::Executor;
use crate::runner::{RunConfig, RunError, RunOutcome, run};

pub struct Scheduler {
    dags: DashMap<String, DagDef>,
    executor: Arc<dyn Executor>,
}

#[derive(Debug, thiserror::Error)]
pub enum TriggerError {
    #[error("DAG '{0}' is not registered with the scheduler")]
    UnknownDag(String),
}

impl Scheduler {
    pub fn new(executor: Arc<dyn Executor>) -> Self {
        Self {
            dags: DashMap::new(),
            executor,
        }
    }

    pub fn register(&self, dag: DagDef) {
        self.dags.insert(dag.dag_id().to_string(), dag);
    }

    /// Spawn a run task for `dag_id` and return a handle the caller can await.
    pub fn trigger(
        &self,
        dag_id: &str,
    ) -> Result<tokio::task::JoinHandle<Result<RunOutcome, RunError>>, TriggerError> {
        let dag = self
            .dags
            .get(dag_id)
            .ok_or_else(|| TriggerError::UnknownDag(dag_id.to_string()))?
            .clone();
        let config = RunConfig {
            dag,
            executor: Arc::clone(&self.executor),
        };
        Ok(tokio::task::spawn(run(config)))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagDef;
    use crate::executor::LocalExecutor;
    use std::collections::HashMap;

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
    async fn trigger_known_dag_succeeds() {
        let executor = bash_executor().await;
        let scheduler = Scheduler::new(executor);
        let dag = compile_dag(
            r#"
dag("test")
bash_operator("a", cmd="printf '{\"outputs\":{}}'  ")
"#,
        );
        scheduler.register(dag);
        let handle = scheduler.trigger("test").unwrap();
        let outcome = handle.await.unwrap().unwrap();
        assert_eq!(outcome.succeeded, vec!["a"]);
    }

    #[tokio::test]
    async fn trigger_unknown_dag_returns_error() {
        let executor = bash_executor().await;
        let scheduler = Scheduler::new(executor);
        let err = scheduler.trigger("nonexistent").unwrap_err();
        assert!(matches!(err, TriggerError::UnknownDag(ref id) if id == "nonexistent"));
    }

    #[tokio::test]
    async fn trigger_failing_task_propagates_error() {
        let executor = bash_executor().await;
        let scheduler = Scheduler::new(executor);
        let dag = compile_dag(
            r#"
dag("test")
bash_operator("a", cmd="exit 1")
"#,
        );
        scheduler.register(dag);
        let handle = scheduler.trigger("test").unwrap();
        let err = handle.await.unwrap().unwrap_err();
        assert!(matches!(err, RunError::TaskFailed { ref node_id, .. } if node_id == "a"));
    }
}
