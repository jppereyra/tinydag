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

use crate::dag::{DagDef, NodeId};
use crate::operators::Operator as _;

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
}

/// Validate a `DagDef`. Returns the topological order on success.
///
/// Runs structural checks (duplicates, unknown edges, connectivity, acyclicity)
/// and operator-level validation (bash -n, python3 -m py_compile).
/// Called by the compiler after resolve; script paths are already absolute.
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

    // --- Task reference validation ---
    for node in &dag.nodes {
        if let Some(reason) = node.task_ref.validate() {
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

    Ok(topo_order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{
        DagDef, Edge, Node, RetryPolicy, TaskRef,
    };
    use crate::operators::BashOperator;

    fn bash_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            task_ref: TaskRef::Bash(BashOperator { cmd: Some("echo ok".to_string()), script: None }),
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
        dag.nodes.push(bash_node("a"));
        assert_eq!(validate(&dag), Ok(vec!["a".to_string()]));
    }

    #[test]
    fn linear_chain_returns_topo_order() {
        let mut dag = DagDef::new("chain");
        dag.nodes.extend([bash_node("a"), bash_node("b"), bash_node("c")]);
        dag.edges.extend([edge("a", "b"), edge("b", "c")]);
        assert_eq!(validate(&dag), Ok(vec!["a", "b", "c"].into_iter().map(str::to_string).collect()));
    }

    #[test]
    fn diamond_dag_is_valid() {
        // a -> b, a -> c, b -> d, c -> d
        let mut dag = DagDef::new("diamond");
        dag.nodes.extend([bash_node("a"), bash_node("b"), bash_node("c"), bash_node("d")]);
        dag.edges.extend([edge("a", "b"), edge("a", "c"), edge("b", "d"), edge("c", "d")]);
        let order = validate(&dag).unwrap();
        assert_eq!(order[0], "a");
        assert_eq!(order[3], "d");
    }

    #[test]
    fn cycle_is_detected() {
        let mut dag = DagDef::new("cycle");
        dag.nodes.extend([bash_node("a"), bash_node("b"), bash_node("c")]);
        dag.edges.extend([edge("a", "b"), edge("b", "c"), edge("c", "a")]);
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::CycleDetected(_)))));
    }

    #[test]
    fn unknown_node_in_edge_is_detected() {
        let mut dag = DagDef::new("unknown");
        dag.nodes.push(bash_node("a"));
        dag.edges.push(edge("a", "ghost"));
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::UnknownNode(id) if id == "ghost"))));
    }

    #[test]
    fn duplicate_node_id_is_detected() {
        let mut dag = DagDef::new("dupe");
        dag.nodes.extend([bash_node("a"), bash_node("a")]);
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::DuplicateNodeId(id) if id == "a"))));
    }

    #[test]
    fn disconnected_node_in_multi_node_dag_is_invalid() {
        let mut dag = DagDef::new("disconnected");
        dag.nodes.extend([bash_node("a"), bash_node("b"), bash_node("orphan")]);
        dag.edges.push(edge("a", "b"));
        assert!(matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::DisconnectedNode(id) if id == "orphan"))));
    }

    #[test]
    fn single_node_with_no_edges_is_valid() {
        let mut dag = DagDef::new("solo");
        dag.nodes.push(bash_node("a"));
        assert!(validate(&dag).is_ok());
    }

    #[test]
    fn validation_handles_multiple_nodes() {
        let mut dag = DagDef::new("multi");
        for id in &["extract", "clean", "load"] {
            dag.nodes.push(Node {
                id: id.to_string(),
                task_ref: TaskRef::Bash(BashOperator { cmd: Some("echo ok".to_string()), script: None }),
                retry: RetryPolicy::default(),
                timeout_secs: None,
            });
        }
        dag.edges.extend([edge("extract", "clean"), edge("clean", "load")]);
        assert!(validate(&dag).is_ok());
    }
}
