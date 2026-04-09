//! Intermediate Representation (IR) for tinydag DAGs.
//!
//! The IR is the contract between the Python DSL (or any external system) and
//! the Rust orchestrator. It must be serializable, versionable, and free of any
//! execution logic.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A unique identifier for a node within a DAG.
pub type NodeId = String;

/// The artifact that a task produces or consumes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Port {
    pub name: String,
    /// Optional type annotation — e.g. "pd.DataFrame", "str", "int".
    /// Not enforced in v1; stored for future contract checking.
    pub type_hint: Option<String>,
}

/// How the task is invoked.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskRef {
    /// A Python callable identified by its dotted module path, e.g. `"mypackage.tasks.clean"`.
    PythonCallable { module_path: String },
    /// A shell script at the given path.
    Script { path: String },
    /// A container image + optional entrypoint override.
    Container { image: String, entrypoint: Option<Vec<String>> },
}

/// Retry policy for a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    /// Delay between attempts in seconds.
    pub delay_secs: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy { max_attempts: 1, delay_secs: 0 }
    }
}

/// What wakes this pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    /// Triggered manually by a user or API call.
    Manual,
    /// Triggered on a cron schedule. `schedule` is a standard cron expression.
    Cron { schedule: String },
    /// Triggered by an external event. `event_type` is an opaque string defined by the producer.
    Event { event_type: String },
    /// Triggered when an upstream pipeline completes successfully.
    /// `upstream_pipeline_id` identifies the pipeline this one depends on.
    PipelineCompletion { upstream_pipeline_id: String },
}

impl Default for Trigger {
    fn default() -> Self {
        Trigger::Manual
    }
}

/// A single node in the DAG.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub task_ref: TaskRef,
    /// Declared input ports. Order is not significant.
    pub inputs: Vec<Port>,
    /// Declared output ports. Order is not significant.
    pub outputs: Vec<Port>,
    pub retry: RetryPolicy,
    /// Timeout in seconds. None = no timeout.
    pub timeout_secs: Option<u64>,
}

/// A directed edge: `from` must complete before `to` can start.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
}

/// The complete DAG definition — the top-level IR type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DagDef {
    /// Schema version for forward-compatibility checks.
    pub version: u32,
    /// Unique identifier for this DAG within its pipeline.
    pub dag_id: String,
    /// The pipeline this DAG belongs to. Distinct from `dag_id`; multiple DAGs
    /// can share a pipeline (e.g. preprocessing + training + evaluation).
    pub pipeline_id: String,
    /// Content hash of the DAG's structural definition. A new version means a new hash.
    /// Set by calling [`DagDef::compute_version_hash`] after building the definition.
    /// Runs in flight belong to the version they started on.
    pub version_hash: String,
    /// What wakes this pipeline.
    pub trigger: Trigger,
    /// The team that owns this pipeline. Required for multi-tenant observability.
    pub team: String,
    /// The user who submitted this pipeline definition.
    pub user: String,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    /// Arbitrary key-value metadata (description, tags, etc.).
    pub metadata: HashMap<String, String>,
}

impl DagDef {
    pub fn new(dag_id: impl Into<String>) -> Self {
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
        }
    }

    /// Compute a deterministic SHA-256 hash over the DAG's structural content:
    /// dag_id, pipeline_id, sorted node IDs, and sorted edges.
    ///
    /// team, user, and metadata are intentionally excluded — they describe who
    /// submitted the DAG, not what the DAG is. Two submissions of the same DAG
    /// by different users should produce the same hash.
    ///
    /// Call this after fully assembling the DAG and store the result in
    /// `version_hash` before persisting or dispatching.
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
            hasher.update(id.as_bytes());
            hasher.update(b"\x00");
        }

        // Edges: sort (from, to) pairs.
        let mut edges: Vec<(&str, &str)> =
            self.edges.iter().map(|e| (e.from.as_str(), e.to.as_str())).collect();
        edges.sort_unstable();
        for (from, to) in &edges {
            hasher.update(from.as_bytes());
            hasher.update(b"\x01");
            hasher.update(to.as_bytes());
            hasher.update(b"\x00");
        }

        format!("{:x}", hasher.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn python_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            task_ref: TaskRef::PythonCallable { module_path: format!("tasks.{id}") },
            inputs: vec![],
            outputs: vec![],
            retry: RetryPolicy::default(),
            timeout_secs: None,
        }
    }

    #[test]
    fn version_hash_is_deterministic() {
        let mut dag = DagDef::new("train");
        dag.pipeline_id = "ml-pipeline".to_string();
        dag.nodes.extend([python_node("a"), python_node("b")]);
        dag.edges.push(Edge { from: "a".to_string(), to: "b".to_string() });

        let h1 = dag.compute_version_hash();
        let h2 = dag.compute_version_hash();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // SHA-256 hex
    }

    #[test]
    fn version_hash_changes_when_node_added() {
        let mut dag = DagDef::new("train");
        dag.pipeline_id = "ml-pipeline".to_string();
        dag.nodes.push(python_node("a"));

        let h1 = dag.compute_version_hash();
        dag.nodes.push(python_node("b"));
        let h2 = dag.compute_version_hash();

        assert_ne!(h1, h2);
    }

    #[test]
    fn version_hash_ignores_team_and_user() {
        let mut dag1 = DagDef::new("train");
        dag1.pipeline_id = "ml-pipeline".to_string();
        dag1.team = "team-a".to_string();
        dag1.user = "alice".to_string();
        dag1.nodes.push(python_node("a"));

        let mut dag2 = dag1.clone();
        dag2.team = "team-b".to_string();
        dag2.user = "bob".to_string();

        assert_eq!(dag1.compute_version_hash(), dag2.compute_version_hash());
    }

    #[test]
    fn version_hash_is_order_independent_for_nodes() {
        let mut dag1 = DagDef::new("train");
        dag1.nodes.extend([python_node("a"), python_node("b")]);

        let mut dag2 = DagDef::new("train");
        dag2.nodes.extend([python_node("b"), python_node("a")]);

        assert_eq!(dag1.compute_version_hash(), dag2.compute_version_hash());
    }

    #[test]
    fn trigger_serialization_roundtrip() {
        let cases = vec![
            Trigger::Manual,
            Trigger::Cron { schedule: "0 6 * * *".to_string() },
            Trigger::Event { event_type: "data.arrived".to_string() },
            Trigger::PipelineCompletion { upstream_pipeline_id: "ingest-pipeline".to_string() },
        ];
        for trigger in cases {
            let json = serde_json::to_string(&trigger).unwrap();
            let back: Trigger = serde_json::from_str(&json).unwrap();
            assert_eq!(trigger, back);
        }
    }

    #[test]
    fn dag_def_roundtrip_with_all_fields() {
        let mut dag = DagDef::new("train");
        dag.pipeline_id = "ml-pipeline".to_string();
        dag.trigger = Trigger::Cron { schedule: "0 6 * * *".to_string() };
        dag.team = "data-eng".to_string();
        dag.user = "alice".to_string();
        dag.nodes.extend([python_node("a"), python_node("b")]);
        dag.edges.push(Edge { from: "a".to_string(), to: "b".to_string() });
        dag.version_hash = dag.compute_version_hash();

        let json = serde_json::to_string(&dag).unwrap();
        let back: DagDef = serde_json::from_str(&json).unwrap();
        assert_eq!(dag, back);
    }
}
