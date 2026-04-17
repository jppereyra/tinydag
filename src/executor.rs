//! Task dispatch
//!
//! The executor owns a tasks and it associated processes, it dispatches tasks
//! to operator binaries and receives outcomes back.

use std::collections::HashMap;
use std::process::Stdio;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use opentelemetry::{KeyValue, global};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use crate::dag::{Node, NodeId, TaskRef};
use crate::operators::Operator;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Everything an executor needs to dispatch a single task.
/// Serialized as JSON and written to the operator binary's stdin.
#[derive(Debug, Clone, Serialize)]
pub struct DispatchPayload {
    pub run_id: String,
    pub dag_id: String,
    pub pipeline_id: String,
    pub dag_version: String,
    pub team: String,
    pub user: String,
    pub trigger_type: String,
    pub node_id: String,
    pub task_ref: TaskRef,
    pub inputs: HashMap<String, Value>,
    pub dag_params: HashMap<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

impl DispatchPayload {
    #[allow(clippy::too_many_arguments)]
    pub fn from_node(
        node: &Node,
        run_id: impl Into<String>,
        dag_id: impl Into<String>,
        pipeline_id: impl Into<String>,
        dag_version: impl Into<String>,
        team: impl Into<String>,
        user: impl Into<String>,
        trigger_type: impl Into<String>,
        inputs: HashMap<String, Value>,
        dag_params: HashMap<String, Value>,
    ) -> Self {
        DispatchPayload {
            run_id: run_id.into(),
            dag_id: dag_id.into(),
            pipeline_id: pipeline_id.into(),
            dag_version: dag_version.into(),
            team: team.into(),
            user: user.into(),
            trigger_type: trigger_type.into(),
            node_id: node.id.clone(),
            task_ref: node.task_ref.clone(),
            inputs,
            dag_params,
            timeout_secs: node.timeout_secs,
        }
    }
}

/// The result of a successful task execution.
#[derive(Debug, Clone)]
pub struct TaskOutcome {
    pub node_id: NodeId,
    pub outputs: HashMap<String, Value>,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("node '{node_id}': task failed (exit {code}): {stderr}")]
    TaskFailed {
        node_id: NodeId,
        code: i32,
        stderr: String,
    },

    #[error("node '{node_id}': task timed out after {timeout_secs}s")]
    TaskTimedOut { node_id: NodeId, timeout_secs: u64 },

    #[error("node '{node_id}': failed to spawn operator binary '{binary}': {source}")]
    SpawnFailed {
        node_id: NodeId,
        binary: String,
        #[source]
        source: std::io::Error,
    },

    #[error("node '{node_id}': failed to write dispatch payload to operator stdin: {source}")]
    StdinFailed {
        node_id: NodeId,
        #[source]
        source: std::io::Error,
    },

    #[error("node '{node_id}': failed to parse operator stdout as JSON: {reason}")]
    OutputParseFailed { node_id: NodeId, reason: String },

    #[error("node '{node_id}': no binary registered for operator type '{operator}'")]
    UnregisteredOperator { node_id: NodeId, operator: String },
}

/// A backend that can dispatch individual tasks.
///
/// Each call to `dispatch` resolves when the task completes (or times out).
/// Parallelism across tasks is the caller's responsibility.
#[async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn dispatch(&self, payload: DispatchPayload) -> Result<TaskOutcome, ExecutorError>;
}

// ---------------------------------------------------------------------------
// LocalExecutor
// ---------------------------------------------------------------------------

/// Runs tasks by spawning operator binaries as child processes on the local machine.
///
/// Each operator type maps to an executable. The default binary name for operator
/// type `X` is `tinydag-op-X` (looked up on `PATH`), overridable via the
/// `TINYDAG_OP_<X>` environment variable (e.g. `TINYDAG_OP_PYTHON=/usr/bin/python3`).
pub struct LocalExecutor {
    /// Maps operator type name to the path to the operator binary.
    registry: HashMap<String, String>,
    /// How long to wait between heartbeats before declaring a task stuck.
    pub heartbeat_timeout_secs: u64,
    control_server: std::sync::Arc<crate::control_server::ControlServer>,
}

impl LocalExecutor {
    /// Build a `LocalExecutor` from the environment.
    ///
    /// For each known operator type, checks `TINYDAG_OP_<TYPE>` first,
    /// then falls back to `tinydag-op-<type>` as the default binary name.
    pub async fn new() -> Self {
        // FIXME: Replace this with a more robust plugin system.
        let known = ["python", "bash"];
        let registry = known
            .iter()
            .map(|&op| {
                let env_key = format!("TINYDAG_OP_{}", op.to_uppercase());
                let binary = std::env::var(&env_key).unwrap_or_else(|_| format!("tinydag-op-{op}"));
                (op.to_string(), binary)
            })
            .collect();
        let control_server = std::sync::Arc::new(
            crate::control_server::ControlServer::start()
                .await
                .expect("failed to start gRPC control server"),
        );
        LocalExecutor {
            registry,
            heartbeat_timeout_secs: 90,
            control_server,
        }
    }

    /// Build a `LocalExecutor` with an explicit registry.
    /// Primarily useful in tests.
    pub async fn with_registry(registry: HashMap<String, String>) -> Self {
        let control_server = std::sync::Arc::new(
            crate::control_server::ControlServer::start()
                .await
                .expect("failed to start gRPC control server"),
        );
        LocalExecutor {
            registry,
            heartbeat_timeout_secs: 90,
            control_server,
        }
    }
}

#[async_trait]
impl Executor for LocalExecutor {
    #[tracing::instrument(
        skip(self, payload),
        fields(
            run.id        = %payload.run_id,
            dag.id        = %payload.dag_id,
            node.id       = %payload.node_id,
            operator.type = %payload.task_ref.type_name(),
        )
    )]
    async fn dispatch(&self, payload: DispatchPayload) -> Result<TaskOutcome, ExecutorError> {
        let start = Instant::now();
        let operator = payload.task_ref.type_name();

        let binary = self
            .registry
            .get(operator)
            .ok_or_else(|| ExecutorError::UnregisteredOperator {
                node_id: payload.node_id.clone(),
                operator: operator.to_string(),
            })?
            .to_string();

        let result = self.spawn_operator(&binary, &payload).await;

        global::meter("tinydag")
            .f64_histogram("tinydag.task.duration")
            .with_unit("s")
            .with_description(
                "Duration of a single task dispatch including operator subprocess lifetime",
            )
            .build()
            .record(
                start.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("dag.id", payload.dag_id.clone()),
                    KeyValue::new("node.id", payload.node_id.clone()),
                    KeyValue::new("operator.type", operator),
                    KeyValue::new("result", if result.is_ok() { "success" } else { "failure" }),
                ],
            );

        result
    }
}

impl LocalExecutor {
    async fn spawn_operator(
        &self,
        binary: &str,
        payload: &DispatchPayload,
    ) -> Result<TaskOutcome, ExecutorError> {
        use crate::control_server::ControlEvent;

        let stdin_json =
            serde_json::to_string(payload).expect("DispatchPayload serialization should not fail");

        // Register before spawning — guard removes the entry on drop.
        let (mut event_rx, _task_guard) = self
            .control_server
            .register(payload.run_id.clone(), payload.node_id.clone());

        let mut child = Command::new(binary)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .env("TINYDAG_CONTROL_ENDPOINT", self.control_server.endpoint())
            .spawn()
            .map_err(|e| ExecutorError::SpawnFailed {
                node_id: payload.node_id.clone(),
                binary: binary.to_string(),
                source: e,
            })?;

        {
            let mut stdin = child.stdin.take().unwrap();
            stdin.write_all(stdin_json.as_bytes()).await.map_err(|e| {
                ExecutorError::StdinFailed {
                    node_id: payload.node_id.clone(),
                    source: e,
                }
            })?;
        }

        let start = Instant::now();
        let heartbeat_timeout = Duration::from_secs(self.heartbeat_timeout_secs);

        loop {
            // Enforce task-level timeout on top of heartbeat timeout.
            let wait_dur = match payload.timeout_secs {
                Some(t) => {
                    let remaining = Duration::from_secs(t).saturating_sub(start.elapsed());
                    remaining.min(heartbeat_timeout)
                }
                None => heartbeat_timeout,
            };

            match tokio::time::timeout(wait_dur, event_rx.recv()).await {
                Ok(Some(ControlEvent::Started)) => {
                    tracing::debug!(node.id = %payload.node_id, "operator reported started");
                }

                Ok(Some(ControlEvent::Heartbeat)) => {
                    tracing::debug!(node.id = %payload.node_id, "operator heartbeat");
                }

                Ok(Some(ControlEvent::Succeeded { outputs_json })) => {
                    let _ = child.wait().await;

                    let outputs: HashMap<String, Value> = if outputs_json.is_empty() {
                        HashMap::new()
                    } else {
                        serde_json::from_str(&outputs_json).map_err(|e| {
                            ExecutorError::OutputParseFailed {
                                node_id: payload.node_id.clone(),
                                reason: format!("invalid outputs JSON in SucceededEvent: {e}"),
                            }
                        })?
                    };

                    return Ok(TaskOutcome {
                        node_id: payload.node_id.clone(),
                        outputs,
                        exit_code: Some(0),
                    });
                }

                Ok(Some(ControlEvent::Failed {
                    error_type,
                    message,
                    exit_code,
                })) => {
                    use crate::control_server::proto::FailedErrorType;
                    let _ = child.wait().await;

                    if error_type == FailedErrorType::InvalidOutput {
                        return Err(ExecutorError::OutputParseFailed {
                            node_id: payload.node_id.clone(),
                            reason: message,
                        });
                    }
                    return Err(ExecutorError::TaskFailed {
                        node_id: payload.node_id.clone(),
                        code: exit_code,
                        stderr: String::new(),
                    });
                }

                Err(_elapsed) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    let timeout_secs = payload.timeout_secs.unwrap_or(self.heartbeat_timeout_secs);
                    return Err(ExecutorError::TaskTimedOut {
                        node_id: payload.node_id.clone(),
                        timeout_secs,
                    });
                }

                Ok(None) => {
                    let _ = child.wait().await;
                    return Err(ExecutorError::SpawnFailed {
                        node_id: payload.node_id.clone(),
                        binary: binary.to_string(),
                        source: std::io::Error::other("control server channel unexpectedly closed"),
                    });
                }
            }

            // If the task-level timeout has elapsed (even with ongoing heartbeats), abort.
            if let Some(t) = payload.timeout_secs
                && start.elapsed() >= Duration::from_secs(t)
            {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(ExecutorError::TaskTimedOut {
                    node_id: payload.node_id.clone(),
                    timeout_secs: t,
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::TaskRef;
    use crate::operators::BashOperator;

    /// Returns a LocalExecutor backed by the real tinydag-op-bash binary.
    ///
    /// Using the real binary avoids creating temp executable files, which
    /// causes ETXTBSY races on Linux when concurrent tests write and exec
    /// the same inode. cargo test builds all bin targets before running
    /// tests, so the binary is always present in target/{profile}/.
    async fn bash_executor() -> LocalExecutor {
        let test_bin = std::env::current_exe().expect("can't locate test binary");
        // test binary  → target/debug/deps/<name>
        // op binary    → target/debug/tinydag-op-bash
        let binary = test_bin
            .parent()
            .unwrap() // …/deps
            .parent()
            .unwrap() // …/debug
            .join("tinydag-op-bash");
        let mut registry = HashMap::new();
        registry.insert("bash".to_string(), binary.to_string_lossy().into_owned());
        LocalExecutor::with_registry(registry).await
    }

    /// Build a bash dispatch payload whose cmd drives the operator's behaviour.
    fn bash_payload(id: &str, cmd: &str, timeout: Option<u64>) -> DispatchPayload {
        DispatchPayload {
            run_id: "run-1".to_string(),
            dag_id: "test-dag".to_string(),
            pipeline_id: "test-pipeline".to_string(),
            dag_version: "abc123".to_string(),
            team: "test-team".to_string(),
            user: "test-user".to_string(),
            trigger_type: "manual".to_string(),
            node_id: id.to_string(),
            task_ref: TaskRef::Bash(BashOperator {
                cmd: Some(cmd.to_string()),
                script: None,
            }),
            inputs: HashMap::new(),
            dag_params: HashMap::new(),
            timeout_secs: timeout,
        }
    }

    #[tokio::test]
    async fn successful_operator_returns_outputs() {
        let ex = bash_executor().await;
        let outcome = ex
            .dispatch(bash_payload(
                "node-1",
                r#"printf '{"outputs":{"x":42}}'"#,
                None,
            ))
            .await
            .unwrap();
        assert_eq!(outcome.node_id, "node-1");
        assert_eq!(outcome.exit_code, Some(0));
        assert_eq!(outcome.outputs["x"], serde_json::json!(42));
    }

    #[tokio::test]
    async fn empty_stdout_is_valid() {
        let ex = bash_executor().await;
        let outcome = ex
            .dispatch(bash_payload("node-1", ":", None))
            .await
            .unwrap();
        assert!(outcome.outputs.is_empty());
    }

    #[tokio::test]
    async fn non_zero_exit_returns_task_failed() {
        let ex = bash_executor().await;
        let err = ex
            .dispatch(bash_payload("node-1", "exit 42", None))
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutorError::TaskFailed { code: 42, .. }));
    }

    #[tokio::test]
    async fn timeout_returns_timed_out() {
        let ex = bash_executor().await;
        let err = ex
            .dispatch(bash_payload("node-1", "sleep 60", Some(1)))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ExecutorError::TaskTimedOut {
                timeout_secs: 1,
                ..
            }
        ));
    }

    #[test]
    fn dispatch_payload_serializes_task_ref() {
        let payload = bash_payload("n1", "echo hi", None);
        let json = serde_json::to_string(&payload).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["task_ref"]["operator_type"], "bash");
        assert_eq!(v["node_id"], "n1");
    }

    #[tokio::test]
    async fn invalid_stdout_returns_parse_error() {
        let ex = bash_executor().await;
        let err = ex
            .dispatch(bash_payload("node-1", "echo not-json", None))
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutorError::OutputParseFailed { .. }));
    }

    // -----------------------------------------------------------------------
    // gRPC protocol tests
    // -----------------------------------------------------------------------

    /// Round-trip: outputs from a successful task flow back to the caller.
    #[tokio::test]
    async fn grpc_successful_task_outputs_flow_back() {
        let ex = bash_executor().await;
        let outcome = ex
            .dispatch(bash_payload(
                "grpc-ok",
                r#"printf '{"outputs":{"result":"hello","n":7}}'"#,
                None,
            ))
            .await
            .unwrap();
        assert_eq!(outcome.outputs["result"], serde_json::json!("hello"));
        assert_eq!(outcome.outputs["n"], serde_json::json!(7));
    }

    /// Round-trip: a failing task returns TaskFailed via gRPC.
    #[tokio::test]
    async fn grpc_failed_task_returns_task_failed() {
        let ex = bash_executor().await;
        let err = ex
            .dispatch(bash_payload("grpc-fail", "exit 7", None))
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutorError::TaskFailed { code: 7, .. }));
    }

    /// Heartbeat timeout: a task that never reports back is killed once the
    /// heartbeat window expires. We set a very short timeout via the task's
    /// `timeout_secs` field so the test doesn't take 30 s.
    #[tokio::test]
    async fn grpc_heartbeat_timeout_kills_stuck_task() {
        let ex = bash_executor().await;
        let err = ex
            .dispatch(bash_payload("grpc-stuck", "sleep 60", Some(2)))
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutorError::TaskTimedOut { .. }));
    }

    /// When TINYDAG_CONTROL_ENDPOINT is absent op-bash must exit non-zero.
    #[tokio::test]
    async fn missing_endpoint_exits_with_error() {
        use std::process::Command;

        let test_bin = std::env::current_exe().expect("can't locate test binary");
        let binary = test_bin
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tinydag-op-bash");

        let payload = bash_payload("node-1", "echo hi", None);
        let stdin_json = serde_json::to_string(&payload).unwrap();

        let output = Command::new(&binary)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .env_remove("TINYDAG_CONTROL_ENDPOINT")
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .unwrap()
                    .write_all(stdin_json.as_bytes())
                    .unwrap();
                child.wait_with_output()
            })
            .unwrap();

        assert!(
            !output.status.success(),
            "should exit non-zero without endpoint"
        );
    }
}
