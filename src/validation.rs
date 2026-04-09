//! DAG validation: structural correctness checks that run at "compile time"
//! (i.e. before any task is submitted for execution).
//!
//! Layer 1 of the fail-fast stack:
//!   - All edge endpoints reference known nodes
//!   - No duplicate node IDs
//!   - Graph is acyclic (Kahn's algorithm)

use std::collections::{HashMap, HashSet, VecDeque};
use thiserror::Error;

use crate::ir::{DagDef, NodeId};

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
}

/// Validate a `DagDef`. Returns the topological order on success.
pub fn validate(dag: &DagDef) -> Result<Vec<NodeId>, Vec<ValidationError>> {
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
    use crate::ir::{DagDef, Edge, Node, Port, RetryPolicy, TaskRef};

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
    fn port_roundtrip_serialization() {
        let port = Port { name: "x".to_string(), type_hint: Some("pd.DataFrame".to_string()) };
        let json = serde_json::to_string(&port).unwrap();
        let back: Port = serde_json::from_str(&json).unwrap();
        assert_eq!(port, back);
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
