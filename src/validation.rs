//! DAG validation: structural correctness checks that run at "compile time"
//! (i.e. before any task is submitted for execution).
//!
//! Layer 1 of the fail-fast stack:
//!   - All edge endpoints reference known nodes
//!   - No duplicate node IDs
//!   - Graph is acyclic (Kahn's algorithm)

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

use opentelemetry::{global, KeyValue};
use thiserror::Error;

use crate::ir::{DagDef, NodeId, TaskRef};

#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
    #[error("duplicate node id: '{0}'")]
    DuplicateNodeId(NodeId),

    #[error("edge references unknown node: '{0}'")]
    UnknownNode(NodeId),

    #[error("cycle detected involving node: '{0}'")]
    CycleDetected(NodeId),

    #[error("dag has no nodes")]
    EmptyDag,

    #[error("node '{0}' is disconnected (no edges) in a multi-node dag")]
    DisconnectedNode(NodeId),

    #[error("node '{node_id}' has an invalid task reference: {reason}")]
    InvalidTaskRef { node_id: NodeId, reason: String },

    #[error("python3 unavailable for callable reference validation: {0}")]
    PythonUnavailable(String),
}

/// Validate a `DagDef`. Returns the topological order on success.
#[tracing::instrument(
    skip(dag),
    fields(
        dag.id = %dag.dag_id,
        dag.pipeline_id = %dag.pipeline_id,
        dag.node_count = dag.nodes.len(),
        dag.edge_count = dag.edges.len(),
    )
)]
pub fn validate(dag: &DagDef) -> Result<Vec<NodeId>, Vec<ValidationError>> {
    let start = Instant::now();
    let result = validate_inner(dag);
    global::meter("tinydag")
        .f64_histogram("tinydag.validation.duration")
        .with_unit("s")
        .with_description("Duration of DAG compile-time structural validation")
        .build()
        .record(
            start.elapsed().as_secs_f64(),
            &[
                KeyValue::new("dag.id", dag.dag_id.clone()),
                KeyValue::new("dag.pipeline_id", dag.pipeline_id.clone()),
                KeyValue::new("result", if result.is_ok() { "success" } else { "failure" }),
            ],
        );
    result
}

fn validate_inner(dag: &DagDef) -> Result<Vec<NodeId>, Vec<ValidationError>> {
    let mut errors: Vec<ValidationError> = Vec::new();

    if dag.nodes.is_empty() {
        return Err(vec![ValidationError::EmptyDag]);
    }

    // --- Check for duplicate node IDs ---
    let mut seen: HashSet<&str> = HashSet::new();
    for node in &dag.nodes {
        if !seen.insert(node.id.as_str()) {
            errors.push(ValidationError::DuplicateNodeId(node.id.clone()));
        }
    }

    let known: HashSet<&str> = dag.nodes.iter().map(|n| n.id.as_str()).collect();

    // --- Check that all edge endpoints exist ---
    for edge in &dag.edges {
        if !known.contains(edge.from.as_str()) {
            errors.push(ValidationError::UnknownNode(edge.from.clone()));
        }
        if !known.contains(edge.to.as_str()) {
            errors.push(ValidationError::UnknownNode(edge.to.clone()));
        }
    }

    // --- Disconnected node detection ---
    // In a multi-node DAG every node must participate in at least one edge.
    if dag.nodes.len() > 1 {
        let mut connected: HashSet<&str> = HashSet::new();
        for edge in &dag.edges {
            connected.insert(edge.from.as_str());
            connected.insert(edge.to.as_str());
        }
        for node in &dag.nodes {
            if !connected.contains(node.id.as_str()) {
                errors.push(ValidationError::DisconnectedNode(node.id.clone()));
            }
        }
    }

    // --- Task reference structural validation ---
    for node in &dag.nodes {
        if let Some(reason) = invalid_task_ref(&node.task_ref) {
            errors.push(ValidationError::InvalidTaskRef {
                node_id: node.id.clone(),
                reason,
            });
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    // --- Kahn's algorithm: cycle detection + topological sort ---
    // Build adjacency list and in-degree map.
    let mut in_degree: HashMap<&str, usize> =
        dag.nodes.iter().map(|n| (n.id.as_str(), 0)).collect();
    let mut adj: HashMap<&str, Vec<&str>> =
        dag.nodes.iter().map(|n| (n.id.as_str(), vec![])).collect();

    for edge in &dag.edges {
        adj.get_mut(edge.from.as_str()).unwrap().push(edge.to.as_str());
        *in_degree.get_mut(edge.to.as_str()).unwrap() += 1;
    }

    let mut queue: VecDeque<&str> =
        in_degree.iter().filter(|&(_, &d)| d == 0).map(|(&id, _)| id).collect();

    // Sort for deterministic output.
    let mut queue_vec: Vec<&str> = queue.drain(..).collect();
    queue_vec.sort_unstable();
    queue.extend(queue_vec);

    let mut topo_order: Vec<NodeId> = Vec::with_capacity(dag.nodes.len());

    while let Some(node_id) = queue.pop_front() {
        topo_order.push(node_id.to_string());
        let mut next: Vec<&str> = adj[node_id].clone();
        next.sort_unstable();
        for neighbor in next {
            let deg = in_degree.get_mut(neighbor).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push_back(neighbor);
            }
        }
    }

    if topo_order.len() != dag.nodes.len() {
        // Nodes that never reached in-degree 0 are part of a cycle.
        let in_cycle: Vec<NodeId> = in_degree
            .iter()
            .filter(|&(_, &d)| d > 0)
            .map(|(&id, _)| id.to_string())
            .collect();

        // Report the lexicographically first node for a stable error message.
        let first = in_cycle.iter().min().cloned().unwrap_or_default();
        return Err(vec![ValidationError::CycleDetected(first)]);
    }

    // FIXME: Feels off to run this here. Think more about this.
    // Validate Python callable references via subprocess.
    // Only runs if structural validation passed and Python nodes exist.
    if dag.nodes.iter().any(|n| matches!(n.task_ref, TaskRef::Python(_))) {
        validate_python_refs(dag).map_err(|e| e)?;
    }

    Ok(topo_order)
}

/// Returns `Some(reason)` if the task reference is structurally invalid, `None` if it is fine.
///
/// This is a structural check only: fields must be non-empty and, for Python callables,
/// the module path must be a valid dotted identifier. It does not attempt to import or
/// resolve the callable — that is the responsibility of the Python-side strict validator.
fn invalid_task_ref(task_ref: &TaskRef) -> Option<String> {
    match task_ref {
        TaskRef::Python(c) => {
            if c.callable_ref.trim().is_empty() {
                return Some("python callable_ref is empty".to_string());
            }
            // Identifier syntax is validated by validate_python_refs(), which delegates
            // to Python's own ast.parse so the rules track the Python version.
            None
        }
        TaskRef::Bash(c) => {
            if c.cmd.trim().is_empty() {
                return Some("bash cmd is empty".to_string());
            }
            None
        }
        // v2+ operators: fields are not validated in v1.
        TaskRef::Sql(_) | TaskRef::S3(_) | TaskRef::Http(_) | TaskRef::Kubernetes(_) => None,
    }
}

/// Validate all `PythonCallable` task references in one Python subprocess.
///
/// Collects every unique `module_path` from the DAG, pipes them to a single
/// `python3 -c` process as JSON, and uses Python's own `ast.parse` to validate
/// each dotted path. This keeps identifier rules coupled to the Python version
/// in the user's environment rather than to a regex we maintain.
///
/// Call this after [`validate`] passes. It requires `python3` on `PATH` (or
/// the executable named by `TINYDAG_PYTHON`).
#[tracing::instrument(
    skip(dag),
    fields(
        dag.id = %dag.dag_id,
        dag.pipeline_id = %dag.pipeline_id,
        python.callable_count = tracing::field::Empty,
        python.unique_path_count = tracing::field::Empty,
    )
)]
fn validate_python_refs(dag: &DagDef) -> Result<(), Vec<ValidationError>> {
    let start = Instant::now();
    let result = validate_python_refs_inner(dag);
    global::meter("tinydag")
        .f64_histogram("tinydag.validation.python_refs.duration")
        .with_unit("s")
        .with_description("Duration of Python callable reference validation including subprocess")
        .build()
        .record(
            start.elapsed().as_secs_f64(),
            &[
                KeyValue::new("dag.id", dag.dag_id.clone()),
                KeyValue::new("dag.pipeline_id", dag.pipeline_id.clone()),
                KeyValue::new("result", if result.is_ok() { "success" } else { "failure" }),
            ],
        );
    result
}

fn validate_python_refs_inner(dag: &DagDef) -> Result<(), Vec<ValidationError>> {
    use std::collections::{HashMap, HashSet};
    use std::io::Write;
    use std::process::{Command, Stdio};
    // Collect (node_id, callable_ref) for every Python node.
    let callables: Vec<(&str, &str)> = dag
        .nodes
        .iter()
        .filter_map(|n| {
            if let TaskRef::Python(c) = &n.task_ref {
                Some((n.id.as_str(), c.callable_ref.as_str()))
            } else {
                None
            }
        })
        .collect();

    if callables.is_empty() {
        return Ok(());
    }

    // Deduplicate paths — many nodes may share a module.
    let unique_paths: Vec<&str> = {
        let mut seen = HashSet::new();
        callables.iter().map(|(_, p)| *p).filter(|p| seen.insert(*p)).collect()
    };

    // Record counts on the current span now that we have them.
    tracing::Span::current().record("python.callable_count", callables.len());
    tracing::Span::current().record("python.unique_path_count", unique_paths.len());

    let input =
        serde_json::to_string(&unique_paths).expect("serializing paths to JSON should not fail");

    // The script reads a JSON list of dotted paths from stdin and writes back a JSON
    // object mapping each path to a boolean. Python's isidentifier() handles Unicode
    // and tracks the language version automatically.
    const VALIDATOR: &str = "\
import sys, json, ast
paths = json.load(sys.stdin)
def valid(path):
    try:
        ast.parse(f'import {path}')
        return True
    except SyntaxError:
        return False
print(json.dumps({p: valid(p) for p in paths}))
";

    let python = std::env::var("TINYDAG_PYTHON").unwrap_or_else(|_| "python3".to_string());

    let output = {
        let subprocess_start = Instant::now();
        let _subprocess = tracing::info_span!(
            "python_ref_validator.subprocess",
            python.executable = %python,
            python.path_count = unique_paths.len(),
        )
        .entered();

        let mut child = Command::new(&python)
            .arg("-c")
            .arg(VALIDATOR)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| vec![ValidationError::PythonUnavailable(format!("{python}: {e}"))])?;

        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .map_err(|e| vec![ValidationError::PythonUnavailable(e.to_string())])?;

        let output = child
            .wait_with_output()
            .map_err(|e| vec![ValidationError::PythonUnavailable(e.to_string())])?;

        global::meter("tinydag")
            .f64_histogram("tinydag.validation.python_refs.subprocess.duration")
            .with_unit("s")
            .with_description("Duration of the python3 subprocess used for callable reference validation")
            .build()
            .record(
                subprocess_start.elapsed().as_secs_f64(),
                &[KeyValue::new("python.executable", python.clone())],
            );

        output
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(vec![ValidationError::PythonUnavailable(format!(
            "{python} exited non-zero: {stderr}"
        ))]);
    }

    let results: HashMap<String, bool> = serde_json::from_slice(&output.stdout)
        .map_err(|e| {
            vec![ValidationError::PythonUnavailable(format!(
                "failed to parse python3 output: {e}"
            ))]
        })?;

    let errors: Vec<ValidationError> = callables
        .iter()
        .filter(|(_, path)| !results.get(*path).copied().unwrap_or(false))
        .map(|(node_id, path)| ValidationError::InvalidTaskRef {
            node_id: node_id.to_string(),
            reason: format!("'{path}' is not a valid Python dotted identifier"),
        })
        .collect();

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{
        BashConfig, DagDef, Edge, KubernetesConfig, Node, PythonConfig, RetryPolicy, TaskRef,
    };

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

    fn edge(from: &str, to: &str) -> Edge {
        Edge { from: from.to_string(), to: to.to_string() }
    }

    #[test]
    fn empty_dag_is_invalid() {
        let dag = DagDef::new("empty");
        assert_eq!(validate(&dag), Err(vec![ValidationError::EmptyDag]));
    }

    #[test]
    fn single_node_is_valid() {
        let mut dag = DagDef::new("single");
        dag.nodes.push(python_node("a"));
        assert_eq!(validate(&dag), Ok(vec!["a".to_string()]));
    }

    #[test]
    fn linear_chain_returns_topo_order() {
        let mut dag = DagDef::new("chain");
        dag.nodes.extend([python_node("a"), python_node("b"), python_node("c")]);
        dag.edges.extend([edge("a", "b"), edge("b", "c")]);
        assert_eq!(validate(&dag), Ok(vec!["a", "b", "c"].into_iter().map(str::to_string).collect()));
    }

    #[test]
    fn diamond_dag_is_valid() {
        // a -> b, a -> c, b -> d, c -> d
        let mut dag = DagDef::new("diamond");
        dag.nodes.extend([python_node("a"), python_node("b"), python_node("c"), python_node("d")]);
        dag.edges.extend([edge("a", "b"), edge("a", "c"), edge("b", "d"), edge("c", "d")]);
        let order = validate(&dag).unwrap();
        // a must come first, d must come last
        assert_eq!(order[0], "a");
        assert_eq!(order[3], "d");
    }

    #[test]
    fn cycle_is_detected() {
        let mut dag = DagDef::new("cycle");
        dag.nodes.extend([python_node("a"), python_node("b"), python_node("c")]);
        dag.edges.extend([edge("a", "b"), edge("b", "c"), edge("c", "a")]);
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::CycleDetected(_)))));
    }

    #[test]
    fn unknown_node_in_edge_is_detected() {
        let mut dag = DagDef::new("unknown");
        dag.nodes.push(python_node("a"));
        dag.edges.push(edge("a", "ghost"));
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::UnknownNode(id) if id == "ghost"))));
    }

    #[test]
    fn duplicate_node_id_is_detected() {
        let mut dag = DagDef::new("dupe");
        dag.nodes.extend([python_node("a"), python_node("a")]);
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::DuplicateNodeId(id) if id == "a"))));
    }

    #[test]
    fn disconnected_node_in_multi_node_dag_is_invalid() {
        let mut dag = DagDef::new("disconnected");
        dag.nodes.extend([python_node("a"), python_node("b"), python_node("orphan")]);
        dag.edges.push(edge("a", "b"));
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::DisconnectedNode(id) if id == "orphan"))));
    }

    #[test]
    fn single_node_with_no_edges_is_valid() {
        let mut dag = DagDef::new("solo");
        dag.nodes.push(python_node("a"));
        assert!(validate(&dag).is_ok());
    }

    #[test]
    fn empty_callable_ref_is_invalid() {
        let mut dag = DagDef::new("bad_ref");
        dag.nodes.push(Node {
            id: "x".to_string(),
            task_ref: TaskRef::Python(PythonConfig {
                callable_ref: "".to_string(),
                inputs: vec![],
                outputs: vec![],
            }),
            retry: RetryPolicy::default(),
            timeout_secs: None,
        });
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::InvalidTaskRef { node_id, .. } if node_id == "x"))));
    }

    #[test]
    fn invalid_dotted_paths_rejected_by_python() {
        for bad in &["my-module.fn", "123module.fn", "mod..fn", ".fn", "mod."] {
            let mut dag = DagDef::new("bad_path");
            dag.nodes.push(Node {
                id: "x".to_string(),
                task_ref: TaskRef::Python(PythonConfig {
                    callable_ref: bad.to_string(),
                    inputs: vec![],
                    outputs: vec![],
                }),
                retry: RetryPolicy::default(),
                timeout_secs: None,
            });
            assert!(
                matches!(validate_python_refs(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::InvalidTaskRef { .. }))),
                "expected InvalidTaskRef from validate_python_refs for '{bad}'"
            );
        }
    }

    #[test]
    fn valid_dotted_paths_accepted_by_python() {
        // Includes a Unicode identifier to confirm Python's rules are used, not an ASCII regex.
        for good in &["mymodule.fn", "my_module.tasks.clean", "_private.fn", "a.b.c.d", "résumé.parse"] {
            let mut dag = DagDef::new("good_path");
            dag.nodes.push(Node {
                id: "x".to_string(),
                task_ref: TaskRef::Python(PythonConfig {
                    callable_ref: good.to_string(),
                    inputs: vec![],
                    outputs: vec![],
                }),
                retry: RetryPolicy::default(),
                timeout_secs: None,
            });
            assert!(validate_python_refs(&dag).is_ok(), "expected Ok for module_path '{good}'");
        }
    }

    #[test]
    fn no_python_nodes_skips_subprocess() {
        let mut dag = DagDef::new("bash_only");
        dag.nodes.push(Node {
            id: "x".to_string(),
            task_ref: TaskRef::Bash(BashConfig { cmd: "echo hello".to_string() }),
            retry: RetryPolicy::default(),
            timeout_secs: None,
        });
        assert!(validate_python_refs(&dag).is_ok());
    }

    #[test]
    fn one_subprocess_for_multiple_nodes() {
        // All three nodes are valid — we just verify the function succeeds and doesn't
        // spawn one process per node (observable indirectly: if it did, it would still pass,
        // but this documents the intent).
        let mut dag = DagDef::new("multi");
        for id in &["extract", "clean", "load"] {
            dag.nodes.push(Node {
                id: id.to_string(),
                task_ref: TaskRef::Python(PythonConfig {
                    callable_ref: format!("mymodule.{id}"),
                    inputs: vec![],
                    outputs: vec![],
                }),
                retry: RetryPolicy::default(),
                timeout_secs: None,
            });
        }
        dag.edges.extend([edge("extract", "clean"), edge("clean", "load")]);
        assert!(validate_python_refs(&dag).is_ok());
    }

    #[test]
    fn empty_bash_cmd_is_invalid() {
        let mut dag = DagDef::new("bad_bash");
        dag.nodes.push(Node {
            id: "x".to_string(),
            task_ref: TaskRef::Bash(BashConfig { cmd: "   ".to_string() }),
            retry: RetryPolicy::default(),
            timeout_secs: None,
        });
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::InvalidTaskRef { node_id, .. } if node_id == "x"))));
    }

    #[test]
    fn v2_operators_pass_structural_validation() {
        // v2+ operators are defined but not dispatched in v1 — their fields are
        // not validated at the structural layer.
        let mut dag = DagDef::new("k8s");
        dag.nodes.push(Node {
            id: "x".to_string(),
            task_ref: TaskRef::Kubernetes(KubernetesConfig {
                image: "".to_string(),
                cmd: "".to_string(),
            }),
            retry: RetryPolicy::default(),
            timeout_secs: None,
        });
        assert!(validate(&dag).is_ok());
    }

    #[test]
    fn dag_def_roundtrip_serialization() {
        let mut dag = DagDef::new("roundtrip");
        dag.nodes.extend([python_node("a"), python_node("b")]);
        dag.edges.push(edge("a", "b"));
        let json = serde_json::to_string(&dag).unwrap();
        let back: DagDef = serde_json::from_str(&json).unwrap();
        assert_eq!(dag, back);
    }
}
