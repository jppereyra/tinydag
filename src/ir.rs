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

// ---------------------------------------------------------------------------
// Operator configs
//
// Each operator type owns its own fields. There are no shared fields across
// operator types — the tagged union enforces that exactly one config is set
// per node, mirroring a protobuf `oneof`.
//
// Only `Python` and `Bash` are dispatched in v1. The remaining operator types
// are defined for schema stability so that IR produced by future tooling can
// be round-tripped without loss, but the v1 scheduler will reject them.
// ---------------------------------------------------------------------------

/// Config for a Python callable task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PythonConfig {
    /// Dotted module path to the callable, e.g. `"mypackage.tasks.clean"`.
    pub callable_ref: String,
    /// Named inputs this task reads. Order is not significant.
    pub inputs: Vec<String>,
    /// Named outputs this task produces. Order is not significant.
    pub outputs: Vec<String>,
}

impl PythonConfig {
    /// Returns `Some(reason)` if this config fails compile-time validation.
    pub fn validate_for_compile(&self) -> Option<String> {
        if self.callable_ref.trim().is_empty() {
            return Some("python callable_ref is empty".to_string());
        }
        if !is_valid_dotted_python_identifier(&self.callable_ref) {
            return Some(format!(
                "'{}' is not a valid Python dotted identifier",
                self.callable_ref,
            ));
        }
        None
    }
}

/// Config for a Bash command task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BashConfig {
    /// Shell command to execute.
    pub cmd: String,
}

impl BashConfig {
    /// Returns `Some(reason)` if this config fails compile-time validation.
    pub fn validate_for_compile(&self) -> Option<String> {
        if self.cmd.trim().is_empty() {
            Some("bash cmd is empty".to_string())
        } else {
            None
        }
    }
}

/// Returns `true` if every `.`-separated segment of `path` is a valid Python
/// identifier, as determined by the RustPython parser.
fn is_valid_dotted_python_identifier(path: &str) -> bool {
    use rustpython_parser::{ast, parse, Mode};
    path.split('.').all(|segment| {
        if segment.is_empty() {
            return false;
        }
        matches!(
            parse(segment, Mode::Expression, "<validator>"),
            Ok(ast::Mod::Expression(ref e)) if matches!(e.body.as_ref(), ast::Expr::Name(_))
        )
    })
}

// v2+ operator configs — defined for schema completeness, not dispatched in v1.

/// Config for a SQL query task. v2+.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlConfig {
    /// Connection identifier (DSN, secret name, etc.).
    pub conn: String,
    pub query: String,
}

/// Config for an S3 copy/move task. v2+.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct S3Config {
    pub src: String,
    pub dst: String,
}

/// Config for an HTTP request task. v2+.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HttpConfig {
    pub url: String,
    pub method: String,
}

/// Config for a Kubernetes job task. v2+.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KubernetesConfig {
    pub image: String,
    pub cmd: String,
}

/// How the task is invoked. A tagged union — exactly one operator type and its
/// config are set per node, enforced at the serialization boundary.
///
/// Serialized as `{"operator_type": "<type>", "config": { ... }}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operator_type", content = "config", rename_all = "snake_case")]
pub enum TaskRef {
    Python(PythonConfig),
    Bash(BashConfig),
    // v2+: defined for schema stability, not dispatched in v1.
    Sql(SqlConfig),
    S3(S3Config),
    Http(HttpConfig),
    Kubernetes(KubernetesConfig),
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
    pub retry: RetryPolicy,
    /// Timeout in seconds. `None` = no timeout.
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
            task_ref: TaskRef::Python(PythonConfig {
                callable_ref: format!("tasks.{id}"),
                inputs: vec![],
                outputs: vec![],
            }),
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
    fn task_ref_serialization_roundtrip() {
        let cases: Vec<TaskRef> = vec![
            TaskRef::Python(PythonConfig {
                callable_ref: "mymodule.extract".to_string(),
                inputs: vec![],
                outputs: vec!["raw".to_string()],
            }),
            TaskRef::Bash(BashConfig { cmd: "echo hello".to_string() }),
            TaskRef::Sql(SqlConfig {
                conn: "postgres://localhost/db".to_string(),
                query: "SELECT 1".to_string(),
            }),
            TaskRef::S3(S3Config {
                src: "s3://bucket/in".to_string(),
                dst: "s3://bucket/out".to_string(),
            }),
            TaskRef::Http(HttpConfig {
                url: "https://example.com".to_string(),
                method: "GET".to_string(),
            }),
            TaskRef::Kubernetes(KubernetesConfig {
                image: "my-image:latest".to_string(),
                cmd: "python train.py".to_string(),
            }),
        ];
        for task_ref in cases {
            let json = serde_json::to_string(&task_ref).unwrap();
            let back: TaskRef = serde_json::from_str(&json).unwrap();
            assert_eq!(task_ref, back);
        }
    }

    // -----------------------------------------------------------------------
    // validate_for_compile
    // -----------------------------------------------------------------------

    #[test]
    fn python_config_empty_callable_ref_is_invalid() {
        let cfg = PythonConfig { callable_ref: "".to_string(), inputs: vec![], outputs: vec![] };
        assert!(cfg.validate_for_compile().is_some());
        let cfg_ws = PythonConfig { callable_ref: "   ".to_string(), inputs: vec![], outputs: vec![] };
        assert!(cfg_ws.validate_for_compile().is_some());
    }

    #[test]
    fn python_config_invalid_dotted_paths_rejected() {
        for bad in &["my-module.fn", "123module.fn", "mod..fn", ".fn", "mod."] {
            let cfg = PythonConfig { callable_ref: bad.to_string(), inputs: vec![], outputs: vec![] };
            assert!(
                cfg.validate_for_compile().is_some(),
                "expected validation failure for '{bad}'"
            );
        }
    }

    #[test]
    fn python_config_valid_dotted_paths_accepted() {
        // Includes a Unicode identifier — Python 3 identifier rules apply.
        for good in &["mymodule.fn", "my_module.tasks.clean", "_private.fn", "a.b.c.d", "résumé.parse"] {
            let cfg = PythonConfig { callable_ref: good.to_string(), inputs: vec![], outputs: vec![] };
            assert!(
                cfg.validate_for_compile().is_none(),
                "expected validation success for '{good}'"
            );
        }
    }

    #[test]
    fn bash_config_empty_cmd_is_invalid() {
        assert!(BashConfig { cmd: "".to_string() }.validate_for_compile().is_some());
        assert!(BashConfig { cmd: "   ".to_string() }.validate_for_compile().is_some());
    }

    #[test]
    fn bash_config_valid_cmd() {
        assert!(BashConfig { cmd: "echo hello".to_string() }.validate_for_compile().is_none());
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
