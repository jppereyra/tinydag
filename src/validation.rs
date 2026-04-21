//! DAG validation: structural correctness checks that run at "compile time"
//! (i.e. before any task is submitted for execution).
//!
//! Layer 1 of the fail-fast stack:
//!   - All edge endpoints reference known nodes
//!   - No duplicate node IDs
//!   - Graph is acyclic (Kahn's algorithm)

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

use opentelemetry::{KeyValue, global};
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

    #[error(
        "predecessors of '{node_id}' produce duplicate output name '{output_name}' \
         (declared by both '{predecessor_a}' and '{predecessor_b}')"
    )]
    FanInOutputConflict {
        node_id: NodeId,
        output_name: String,
        predecessor_a: NodeId,
        predecessor_b: NodeId,
    },

    #[error("node '{node_id}' declares input '{input_name}' but no predecessor produces it")]
    UnsatisfiedInput { node_id: NodeId, input_name: String },

    #[error(
        "node '{node_id}' declares inputs {input_names:?} but has no predecessors to provide them"
    )]
    InputOnSourceNode {
        node_id: NodeId,
        input_names: Vec<String>,
    },

    #[error("node '{node_id}' declares output '{output_name}' more than once")]
    DuplicateDeclaredOutput {
        node_id: NodeId,
        output_name: String,
    },
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

    // --- Fan-in output collision check ---
    // For each node, collect declared outputs from all predecessors. If two
    // predecessors declare the same output name, the runner would collide at
    // runtime — reject it here at compile time instead.
    let node_map: HashMap<&str, &crate::dag::Node> =
        dag.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let mut predecessors: HashMap<&str, Vec<&str>> =
        dag.nodes.iter().map(|n| (n.id.as_str(), vec![])).collect();
    for edge in &dag.edges {
        if let Some(preds) = predecessors.get_mut(edge.to.as_str()) {
            preds.push(edge.from.as_str());
        }
    }
    for node in &dag.nodes {
        let preds = &predecessors[node.id.as_str()];
        if preds.len() < 2 {
            continue;
        }
        // output_name -> first predecessor that declared it
        let mut seen: HashMap<&str, &str> = HashMap::new();
        for &pred_id in preds {
            for output_name in node_map[pred_id].task_ref.declared_outputs() {
                if let Some(&first) = seen.get(output_name.as_str()) {
                    errors.push(ValidationError::FanInOutputConflict {
                        node_id: node.id.clone(),
                        output_name: output_name.clone(),
                        predecessor_a: first.to_string(),
                        predecessor_b: pred_id.to_string(),
                    });
                } else {
                    seen.insert(output_name.as_str(), pred_id);
                }
            }
        }
    }

    // --- Build in-degree map (shared by source-node and Kahn's passes) ---
    let mut in_degree: HashMap<&str, usize> =
        dag.nodes.iter().map(|n| (n.id.as_str(), 0)).collect();
    for edge in &dag.edges {
        if let Some(d) = in_degree.get_mut(edge.to.as_str()) {
            *d += 1;
        }
    }

    // --- Duplicate declared outputs on a single node ---
    for node in &dag.nodes {
        let mut seen_outputs: HashSet<&str> = HashSet::new();
        for name in node.task_ref.declared_outputs() {
            if !seen_outputs.insert(name.as_str()) {
                errors.push(ValidationError::DuplicateDeclaredOutput {
                    node_id: node.id.clone(),
                    output_name: name.clone(),
                });
            }
        }
    }

    // --- Input declared on a source node ---
    // A node with no predecessors can never have its declared inputs satisfied.
    for node in &dag.nodes {
        if in_degree[node.id.as_str()] == 0 {
            let inputs: Vec<String> = node.task_ref.declared_inputs().to_vec();
            if !inputs.is_empty() {
                errors.push(ValidationError::InputOnSourceNode {
                    node_id: node.id.clone(),
                    input_names: inputs,
                });
            }
        }
    }

    // --- Unsatisfied inputs ---
    // For each node, the union of its predecessors' declared outputs must cover
    // every input the node declares.
    for node in &dag.nodes {
        let preds = &predecessors[node.id.as_str()];
        let available: HashSet<&str> = preds
            .iter()
            .flat_map(|&pid| node_map[pid].task_ref.declared_outputs())
            .map(String::as_str)
            .collect();
        for input_name in node.task_ref.declared_inputs() {
            if !available.contains(input_name.as_str()) {
                errors.push(ValidationError::UnsatisfiedInput {
                    node_id: node.id.clone(),
                    input_name: input_name.clone(),
                });
            }
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    // --- Kahn's algorithm: cycle detection + topological sort ---
    // Build adjacency list; in_degree was already computed above.
    let mut adj: HashMap<&str, Vec<&str>> =
        dag.nodes.iter().map(|n| (n.id.as_str(), vec![])).collect();

    for edge in &dag.edges {
        adj.get_mut(edge.from.as_str())
            .unwrap()
            .push(edge.to.as_str());
    }

    let mut queue: VecDeque<&str> = in_degree
        .iter()
        .filter(|&(_, &d)| d == 0)
        .map(|(&id, _)| id)
        .collect();

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
    use crate::dag::{DagDef, Edge, Node, RetryPolicy, TaskRef};

    fn bash_node(id: &str) -> Node {
        Node {
            id: id.to_string(),
            task_ref: TaskRef::stub(),
            retry: RetryPolicy::default(),
            timeout_secs: None,
        }
    }

    fn edge(from: &str, to: &str) -> Edge {
        Edge {
            from: from.to_string(),
            to: to.to_string(),
        }
    }

    fn node_with_ref(id: &str, task_ref: TaskRef) -> Node {
        Node {
            id: id.to_string(),
            task_ref,
            retry: RetryPolicy::default(),
            timeout_secs: None,
        }
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
        dag.nodes
            .extend([bash_node("a"), bash_node("b"), bash_node("c")]);
        dag.edges.extend([edge("a", "b"), edge("b", "c")]);
        assert_eq!(
            validate(&dag),
            Ok(vec!["a", "b", "c"]
                .into_iter()
                .map(str::to_string)
                .collect())
        );
    }

    #[test]
    fn diamond_dag_is_valid() {
        // a -> b, a -> c, b -> d, c -> d
        let mut dag = DagDef::new("diamond");
        dag.nodes.extend([
            bash_node("a"),
            bash_node("b"),
            bash_node("c"),
            bash_node("d"),
        ]);
        dag.edges.extend([
            edge("a", "b"),
            edge("a", "c"),
            edge("b", "d"),
            edge("c", "d"),
        ]);
        let order = validate(&dag).unwrap();
        assert_eq!(order[0], "a");
        assert_eq!(order[3], "d");
    }

    #[test]
    fn cycle_is_detected() {
        let mut dag = DagDef::new("cycle");
        dag.nodes
            .extend([bash_node("a"), bash_node("b"), bash_node("c")]);
        dag.edges
            .extend([edge("a", "b"), edge("b", "c"), edge("c", "a")]);
        assert!(
            matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::CycleDetected(_))))
        );
    }

    #[test]
    fn unknown_node_in_edge_is_detected() {
        let mut dag = DagDef::new("unknown");
        dag.nodes.push(bash_node("a"));
        dag.edges.push(edge("a", "ghost"));
        assert!(
            matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::UnknownNode(id) if id == "ghost")))
        );
    }

    #[test]
    fn duplicate_node_id_is_detected() {
        let mut dag = DagDef::new("dupe");
        dag.nodes.extend([bash_node("a"), bash_node("a")]);
        assert!(
            matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::DuplicateNodeId(id) if id == "a")))
        );
    }

    #[test]
    fn disconnected_node_in_multi_node_dag_is_invalid() {
        let mut dag = DagDef::new("disconnected");
        dag.nodes
            .extend([bash_node("a"), bash_node("b"), bash_node("orphan")]);
        dag.edges.push(edge("a", "b"));
        assert!(
            matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(e, ValidationError::DisconnectedNode(id) if id == "orphan")))
        );
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
                task_ref: TaskRef::stub(),
                retry: RetryPolicy::default(),
                timeout_secs: None,
            });
        }
        dag.edges
            .extend([edge("extract", "clean"), edge("clean", "load")]);
        assert!(validate(&dag).is_ok());
    }

    #[test]
    fn duplicate_declared_output_on_node_is_invalid() {
        let mut dag = DagDef::new("dup-out");
        dag.nodes.push(node_with_ref(
            "a",
            TaskRef::stub_with_outputs(vec!["x".to_string(), "x".to_string()]),
        ));
        assert!(
            matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(
                e, ValidationError::DuplicateDeclaredOutput { node_id, output_name }
                if node_id == "a" && output_name == "x"
            )))
        );
    }

    #[test]
    fn input_on_source_node_is_invalid() {
        let mut dag = DagDef::new("src-input");
        dag.nodes.push(node_with_ref(
            "a",
            TaskRef::stub_with_inputs(vec!["x".to_string()]),
        ));
        assert!(
            matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(
                e, ValidationError::InputOnSourceNode { node_id, .. }
                if node_id == "a"
            )))
        );
    }

    #[test]
    fn unsatisfied_input_is_invalid() {
        let mut dag = DagDef::new("unsat");
        dag.nodes.extend([
            node_with_ref("a", TaskRef::stub_with_outputs(vec!["y".to_string()])),
            node_with_ref("b", TaskRef::stub_with_inputs(vec!["x".to_string()])),
        ]);
        dag.edges.push(edge("a", "b"));
        assert!(
            matches!(validate(&dag), Err(ref e) if e.iter().any(|e| matches!(
                e, ValidationError::UnsatisfiedInput { node_id, input_name }
                if node_id == "b" && input_name == "x"
            )))
        );
    }

    #[test]
    fn satisfied_input_passes() {
        let mut dag = DagDef::new("sat");
        dag.nodes.extend([
            node_with_ref("a", TaskRef::stub_with_outputs(vec!["x".to_string()])),
            node_with_ref("b", TaskRef::stub_with_inputs(vec!["x".to_string()])),
        ]);
        dag.edges.push(edge("a", "b"));
        assert!(validate(&dag).is_ok());
    }
}
