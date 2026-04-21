//! DAG type definitions for tinydag.
//!
//! The DAG definition is the contract between the Starlark DSL (or any external system) and
//! the Rust orchestrator. It must be serializable, versionable, and free of any
//! execution logic.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::operators::Operator as _;

// Re-export so callers can use `crate::dag::TaskRef` as before.
pub use crate::operators::TaskRef;

/// A unique identifier for a node within a DAG.
pub type NodeId = String;

/// Retry policy for a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    /// Delay between attempts in seconds.
    pub delay_secs: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 1,
            delay_secs: 0,
        }
    }
}

/// What wakes this pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    /// Triggered manually by a user or API call.
    #[default]
    Manual,
    /// Triggered on a cron schedule. `schedule` is a standard cron expression.
    Cron { schedule: String },
    /// Triggered by an external event. `event_type` is an opaque string defined by the producer.
    Event { event_type: String },
    /// Triggered when an upstream pipeline completes successfully.
    /// `upstream_pipeline_id` identifies the pipeline this one depends on.
    PipelineCompletion { upstream_pipeline_id: String },
}

/// A single node in the DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub task_ref: TaskRef,
    pub retry: RetryPolicy,
    /// Timeout in seconds. `None` = no timeout.
    pub timeout_secs: Option<u64>,
    /// Named inputs this node reads from its predecessors' outputs.
    pub inputs: Vec<String>,
    /// Named outputs this node declares it will produce.
    pub outputs: Vec<String>,
}

/// A directed edge: `from` must complete before `to` can start.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
}

/// The complete DAG definition.
///
/// Constructed exclusively via [`crate::compiler::compile`]. Fields are pub(crate) so
/// internal crate code can read them directly; external consumers use the
/// public getters below.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DagDef {
    /// Schema version for forward-compatibility checks.
    pub(crate) version: u32,
    /// Unique identifier for this DAG within its pipeline.
    pub(crate) dag_id: String,
    /// The pipeline this DAG belongs to. Distinct from `dag_id`; multiple DAGs
    /// can share a pipeline (e.g. preprocessing + training + evaluation).
    pub(crate) pipeline_id: String,
    /// Content hash of the DAG's structural definition. A new version means a new hash.
    /// Set by calling [`DagDef::compute_version_hash`] after building the definition.
    /// Runs in flight belong to the version they started on.
    pub(crate) version_hash: String,
    /// What wakes this pipeline.
    pub(crate) trigger: Trigger,
    /// The team that owns this pipeline. Required for multi-tenant observability.
    pub(crate) team: String,
    /// The user who submitted this pipeline definition.
    pub(crate) user: String,
    pub(crate) nodes: Vec<Node>,
    pub(crate) edges: Vec<Edge>,
    /// Arbitrary key-value metadata (description, tags, etc.).
    pub(crate) metadata: HashMap<String, String>,
    /// Named parameters for this DAG. The full dict is forwarded to every
    /// operator at dispatch time. Affects structural identity (changes the
    /// version hash).
    pub(crate) params: HashMap<String, serde_json::Value>,
}

impl DagDef {
    #[cfg(test)]
    pub(crate) fn new(dag_id: impl Into<String>) -> Self {
        DagDef {
            version: 1,
            dag_id: dag_id.into(),
            pipeline_id: String::new(),
            version_hash: String::new(),
            trigger: Trigger::default(),
            team: String::new(),
            user: String::new(),
            nodes: Vec::new(),
            edges: Vec::new(),
            metadata: HashMap::new(),
            params: HashMap::new(),
        }
    }

    pub fn dag_id(&self) -> &str {
        &self.dag_id
    }
    pub fn pipeline_id(&self) -> &str {
        &self.pipeline_id
    }
    pub fn version(&self) -> u32 {
        self.version
    }
    pub fn version_hash(&self) -> &str {
        &self.version_hash
    }
    pub fn trigger(&self) -> &Trigger {
        &self.trigger
    }
    pub fn team(&self) -> &str {
        &self.team
    }
    pub fn user(&self) -> &str {
        &self.user
    }
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }
    pub fn params(&self) -> &HashMap<String, serde_json::Value> {
        &self.params
    }
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Compute a deterministic SHA-256 hash over the DAG's structural content.
    ///
    /// Covers: dag_id, pipeline_id, sorted node IDs, operator config (type +
    /// field values), operator runtime content (inline cmd text or script file
    /// bytes), sorted edges, and sorted params.
    ///
    /// Call this after resolve() so script paths are absolute and file contents
    /// are stable.
    pub fn compute_version_hash(&self) -> String {
        let mut hasher = Sha256::new();

        hasher.update(self.dag_id.as_bytes());
        hasher.update(b"\x00");
        hasher.update(self.pipeline_id.as_bytes());
        hasher.update(b"\x00");

        // Nodes: sort by ID for determinism regardless of insertion order.
        let mut node_ids: Vec<&str> = self.nodes.iter().map(|n| n.id.as_str()).collect();
        node_ids.sort_unstable();
        for id in &node_ids {
            let node = self.nodes.iter().find(|n| n.id == *id).unwrap();
            hasher.update(id.as_bytes());
            hasher.update(b"\x00");
            // Operator type + field values (script path, cmd).
            hasher.update(
                serde_json::to_string(&node.task_ref)
                    .unwrap_or_default()
                    .as_bytes(),
            );
            hasher.update(b"\x01");
            // Operator runtime content: cmd text or script file bytes.
            hasher.update(node.task_ref.content_for_hash());
            hasher.update(b"\x00");
            // Task-level inputs and outputs (sorted for determinism).
            let mut sorted_inputs = node.inputs.clone();
            sorted_inputs.sort_unstable();
            for name in &sorted_inputs {
                hasher.update(name.as_bytes());
                hasher.update(b"\x03");
            }
            let mut sorted_outputs = node.outputs.clone();
            sorted_outputs.sort_unstable();
            for name in &sorted_outputs {
                hasher.update(name.as_bytes());
                hasher.update(b"\x04");
            }
        }

        // Edges: sort (from, to) pairs.
        let mut edges: Vec<(&str, &str)> = self
            .edges
            .iter()
            .map(|e| (e.from.as_str(), e.to.as_str()))
            .collect();
        edges.sort_unstable();
        for (from, to) in &edges {
            hasher.update(from.as_bytes());
            hasher.update(b"\x01");
            hasher.update(to.as_bytes());
            hasher.update(b"\x00");
        }

        // Params: sort by key for determinism regardless of insertion order.
        let mut param_keys: Vec<&str> = self.params.keys().map(String::as_str).collect();
        param_keys.sort_unstable();
        for key in &param_keys {
            let value = &self.params[*key];
            hasher.update(key.as_bytes());
            hasher.update(b"\x02");
            hasher.update(serde_json::to_string(value).unwrap_or_default().as_bytes());
            hasher.update(b"\x00");
        }

        format!("{:x}", hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stub_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            task_ref: crate::operators::TaskRef::stub(),
            retry: RetryPolicy::default(),
            timeout_secs: None,
            inputs: vec![],
            outputs: vec![],
        }
    }

    #[test]
    fn version_hash_is_deterministic() {
        let mut dag = DagDef::new("train");
        dag.pipeline_id = "ml-pipeline".to_string();
        dag.nodes.extend([stub_node("a"), stub_node("b")]);
        dag.edges.push(Edge {
            from: "a".to_string(),
            to: "b".to_string(),
        });

        let h1 = dag.compute_version_hash();
        let h2 = dag.compute_version_hash();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // SHA-256 hex
    }

    #[test]
    fn version_hash_changes_when_node_added() {
        let mut dag = DagDef::new("train");
        dag.pipeline_id = "ml-pipeline".to_string();
        dag.nodes.push(stub_node("a"));

        let h1 = dag.compute_version_hash();
        dag.nodes.push(stub_node("b"));
        let h2 = dag.compute_version_hash();

        assert_ne!(h1, h2);
    }

    #[test]
    fn version_hash_ignores_team_and_user() {
        let mut dag1 = DagDef::new("train");
        dag1.pipeline_id = "ml-pipeline".to_string();
        dag1.team = "team-a".to_string();
        dag1.user = "alice".to_string();
        dag1.nodes.push(stub_node("a"));

        let mut dag2 = dag1.clone();
        dag2.team = "team-b".to_string();
        dag2.user = "bob".to_string();

        assert_eq!(dag1.compute_version_hash(), dag2.compute_version_hash());
    }

    #[test]
    fn version_hash_is_order_independent_for_nodes() {
        let mut dag1 = DagDef::new("train");
        dag1.nodes.extend([stub_node("a"), stub_node("b")]);

        let mut dag2 = DagDef::new("train");
        dag2.nodes.extend([stub_node("b"), stub_node("a")]);

        assert_eq!(dag1.compute_version_hash(), dag2.compute_version_hash());
    }

    #[test]
    fn trigger_serialization_roundtrip() {
        let cases = vec![
            Trigger::Manual,
            Trigger::Cron {
                schedule: "0 6 * * *".to_string(),
            },
            Trigger::Event {
                event_type: "data.arrived".to_string(),
            },
            Trigger::PipelineCompletion {
                upstream_pipeline_id: "ingest-pipeline".to_string(),
            },
        ];
        for trigger in cases {
            let json = serde_json::to_string(&trigger).unwrap();
            let back: Trigger = serde_json::from_str(&json).unwrap();
            assert_eq!(trigger, back);
        }
    }

    #[test]
    fn dag_params_serializes_to_json() {
        let mut dag = DagDef::new("parameterized");
        dag.params
            .insert("env".to_string(), serde_json::json!("prod"));
        dag.params
            .insert("batch_size".to_string(), serde_json::json!(100));
        dag.params
            .insert("debug".to_string(), serde_json::json!(false));

        let json = serde_json::to_string(&dag).unwrap();
        assert!(!json.is_empty());
        assert_eq!(dag.params()["env"], serde_json::json!("prod"));
        assert_eq!(dag.params()["batch_size"], serde_json::json!(100));
        assert_eq!(dag.params()["debug"], serde_json::json!(false));
    }

    #[test]
    fn dag_params_affect_version_hash() {
        let mut dag1 = DagDef::new("d");
        dag1.nodes.push(stub_node("a"));

        let mut dag2 = dag1.clone();
        dag2.params
            .insert("env".to_string(), serde_json::json!("prod"));

        assert_ne!(dag1.compute_version_hash(), dag2.compute_version_hash());
    }

    #[test]
    fn dag_def_serializes_all_fields() {
        let mut dag = DagDef::new("train");
        dag.pipeline_id = "ml-pipeline".to_string();
        dag.trigger = Trigger::Cron {
            schedule: "0 6 * * *".to_string(),
        };
        dag.team = "data-eng".to_string();
        dag.user = "alice".to_string();
        dag.nodes.extend([stub_node("a"), stub_node("b")]);
        dag.edges.push(Edge {
            from: "a".to_string(),
            to: "b".to_string(),
        });
        dag.version_hash = dag.compute_version_hash();

        let json = serde_json::to_string(&dag).unwrap();
        assert!(json.contains("\"dag_id\":\"train\""));
        assert!(json.contains("\"pipeline_id\":\"ml-pipeline\""));
        assert!(json.contains("\"team\":\"data-eng\""));
    }
}
