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
///
/// Deserializing does not re-run validation or recompute `version_hash`; only
/// load JSON that this crate produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

        let mut nodes: Vec<&Node> = self.nodes.iter().collect();
        nodes.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        for node in &nodes {
            hasher.update(node.id.as_bytes());
            hasher.update(b"\x00");
            node.task_ref.hash_into(&mut hasher);
            hasher.update(b"\x01");
            let mut inputs: Vec<&str> = node.inputs.iter().map(String::as_str).collect();
            inputs.sort_unstable();
            for name in &inputs {
                hasher.update(name.as_bytes());
                hasher.update(b"\x03");
            }
            let mut outputs: Vec<&str> = node.outputs.iter().map(String::as_str).collect();
            outputs.sort_unstable();
            for name in &outputs {
                hasher.update(name.as_bytes());
                hasher.update(b"\x04");
            }
        }

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
    fn version_hash_is_stable_for_nested_param_dicts() {
        // serde_json's Map sorts keys today, so this passes trivially. It guards
        // against the `preserve_order` feature being enabled (possibly by another
        // dependency via feature unification), which would make Map keep
        // insertion order and change the hash.
        let mut m1 = serde_json::Map::new();
        m1.insert("lr".into(), serde_json::json!(0.1));
        m1.insert("batch".into(), serde_json::json!(32));

        let mut m2 = serde_json::Map::new();
        m2.insert("batch".into(), serde_json::json!(32));
        m2.insert("lr".into(), serde_json::json!(0.1));

        let mut dag1 = DagDef::new("train");
        dag1.params
            .insert("model".into(), serde_json::Value::Object(m1));

        let mut dag2 = DagDef::new("train");
        dag2.params
            .insert("model".into(), serde_json::Value::Object(m2));

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
    fn compiled_dag_json_roundtrip_is_lossless() {
        let dag = crate::compiler::compile(
            "test.star",
            r#"
cfg = config(name="rt", schedule="0 6 * * *", params={"env": "prod", "model": {"lr": 0.1}})
a = bash_operator("a", cmd="true", inputs=[], outputs=["x"], timeout_secs=30)
b = bash_operator("b", cmd="true", inputs=["x"], outputs=[], depends_on=a, max_attempts=2)
build(cfg, b)
"#,
            None,
        )
        .unwrap();

        let json = serde_json::to_string(&dag).unwrap();
        let back: DagDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back, dag);
        assert_eq!(back.version_hash(), dag.version_hash());
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
