//! Test doubles shared across unit tests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::Value;

use crate::dag::NodeId;
use crate::executor::{DispatchPayload, Executor, ExecutorError, FailureKind, TaskOutcome};

/// How [`MockExecutor`] answers a dispatch of one node.
#[derive(Clone, Debug)]
pub(crate) enum MockBehavior {
    Succeed(HashMap<String, Value>),
    Fail { code: i32 },
    Panic,
}

/// An in-memory [`Executor`] that records every dispatch and answers from a
/// per-node script. Nodes without a script succeed with no outputs.
#[derive(Default)]
pub(crate) struct MockExecutor {
    behaviors: HashMap<NodeId, MockBehavior>,
    dispatched: Mutex<Vec<DispatchPayload>>,
}

impl MockExecutor {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with(mut self, node_id: &str, behavior: MockBehavior) -> Self {
        self.behaviors.insert(node_id.to_string(), behavior);
        self
    }

    /// Every payload received, in dispatch order.
    pub(crate) fn dispatched(&self) -> Vec<DispatchPayload> {
        self.dispatched.lock().unwrap().clone()
    }

    pub(crate) fn dispatch_order(&self) -> Vec<NodeId> {
        self.dispatched().into_iter().map(|p| p.node_id).collect()
    }
}

#[async_trait]
impl Executor for MockExecutor {
    async fn dispatch(&self, payload: DispatchPayload) -> Result<TaskOutcome, ExecutorError> {
        let node_id = payload.node_id.clone();
        let behavior = self
            .behaviors
            .get(&node_id)
            .cloned()
            .unwrap_or_else(|| MockBehavior::Succeed(HashMap::new()));
        self.dispatched.lock().unwrap().push(payload);
        match behavior {
            MockBehavior::Succeed(outputs) => Ok(TaskOutcome {
                node_id,
                outputs,
                exit_code: Some(0),
            }),
            MockBehavior::Panic => panic!("mock executor panicked dispatching '{node_id}'"),
            MockBehavior::Fail { code } => Err(ExecutorError::TaskFailed {
                node_id,
                kind: FailureKind::TaskFailed,
                code,
                message: format!("mock failure (exit {code})"),
            }),
        }
    }
}

/// Whether `pid` is a live (non-zombie) process.
#[cfg(target_os = "linux")]
pub(crate) fn process_alive(pid: i32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        // Field 3, after the parenthesised command name, is the state.
        Ok(stat) => stat
            .rsplit_once(") ")
            .is_some_and(|(_, rest)| !rest.starts_with('Z')),
        Err(_) => false,
    }
}
