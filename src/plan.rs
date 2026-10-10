//! Precomputed, index-based view of a [`DagDef`] for execution.
//!
//! Built once per DAG version so that running a DAG never searches nodes or
//! edges by id. A node's index is its position in [`DagDef::nodes`], which the
//! compiler fills in a deterministic order, so every ordering derived from a
//! plan (roots, successors, predecessors) is deterministic too.

use std::collections::HashMap;
use std::sync::Arc;

use crate::dag::{DagDef, Node};

/// Adjacency in compressed sparse row form: the neighbours of node `i` are
/// `targets[offsets[i]..offsets[i + 1]]`, in edge declaration order.
struct Csr {
    offsets: Box<[u32]>,
    targets: Box<[u32]>,
}

impl Csr {
    fn build(n: usize, pairs: &[(u32, u32)]) -> Self {
        let mut offsets = vec![0u32; n + 1];
        for &(from, _) in pairs {
            offsets[from as usize + 1] += 1;
        }
        for i in 0..n {
            offsets[i + 1] += offsets[i];
        }
        let mut cursor = offsets[..n].to_vec();
        let mut targets = vec![0u32; pairs.len()];
        for &(from, to) in pairs {
            let slot = &mut cursor[from as usize];
            targets[*slot as usize] = to;
            *slot += 1;
        }
        Csr {
            offsets: offsets.into_boxed_slice(),
            targets: targets.into_boxed_slice(),
        }
    }

    fn of(&self, i: u32) -> &[u32] {
        let i = i as usize;
        &self.targets[self.offsets[i] as usize..self.offsets[i + 1] as usize]
    }
}

pub(crate) struct Plan {
    dag: Arc<DagDef>,
    successors: Csr,
    predecessors: Csr,
    in_degree: Box<[u32]>,
    /// Nodes with no incoming edges, in declaration order.
    roots: Box<[u32]>,
}

impl Plan {
    /// Builds the plan. The `DagDef` must be validated: every edge endpoint
    /// must name a node (guaranteed by [`crate::compiler::compile`]).
    pub(crate) fn new(dag: Arc<DagDef>) -> Self {
        let n = dag.nodes.len();
        let index: HashMap<&str, u32> = dag
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (node.id.as_str(), i as u32))
            .collect();
        let ix = |id: &str| -> u32 {
            *index.get(id).unwrap_or_else(|| {
                panic!("edge endpoint '{id}' is not a node; DagDef not validated")
            })
        };

        let forward: Vec<(u32, u32)> = dag.edges.iter().map(|e| (ix(&e.from), ix(&e.to))).collect();
        let backward: Vec<(u32, u32)> = forward.iter().map(|&(from, to)| (to, from)).collect();

        let mut in_degree = vec![0u32; n];
        for &(_, to) in &forward {
            in_degree[to as usize] += 1;
        }
        let roots = (0..n as u32)
            .filter(|&i| in_degree[i as usize] == 0)
            .collect();

        Plan {
            successors: Csr::build(n, &forward),
            predecessors: Csr::build(n, &backward),
            in_degree: in_degree.into_boxed_slice(),
            roots,
            dag,
        }
    }

    pub(crate) fn dag(&self) -> &DagDef {
        &self.dag
    }

    pub(crate) fn node(&self, i: u32) -> &Node {
        &self.dag.nodes[i as usize]
    }

    pub(crate) fn roots(&self) -> &[u32] {
        &self.roots
    }

    pub(crate) fn in_degree(&self) -> &[u32] {
        &self.in_degree
    }

    pub(crate) fn successors(&self, i: u32) -> &[u32] {
        self.successors.of(i)
    }

    pub(crate) fn predecessors(&self, i: u32) -> &[u32] {
        self.predecessors.of(i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(src: &str) -> Plan {
        Plan::new(Arc::new(
            crate::compiler::compile("test.star", src, None).unwrap(),
        ))
    }

    fn ids<'a>(p: &'a Plan, ixs: &[u32]) -> Vec<&'a str> {
        ixs.iter().map(|&i| p.node(i).id.as_str()).collect()
    }

    #[test]
    fn diamond_adjacency() {
        let p = plan(
            r#"
cfg = config(name="diamond")
a = bash_operator("a", cmd="true", inputs=[], outputs=[])
b = bash_operator("b", cmd="true", inputs=[], outputs=[], depends_on=a)
c = bash_operator("c", cmd="true", inputs=[], outputs=[], depends_on=a)
d = bash_operator("d", cmd="true", inputs=[], outputs=[], depends_on=[b, c])
build(cfg, d)
"#,
        );
        let ix = |id: &str| p.dag().nodes.iter().position(|n| n.id == id).unwrap() as u32;

        assert_eq!(ids(&p, p.roots()), ["a"]);
        assert_eq!(ids(&p, p.successors(ix("a"))), ["b", "c"]);
        assert_eq!(ids(&p, p.predecessors(ix("d"))), ["b", "c"]);
        assert!(p.successors(ix("d")).is_empty());
        assert_eq!(p.in_degree()[ix("d") as usize], 2);
        assert_eq!(p.in_degree()[ix("a") as usize], 0);
    }

    #[test]
    fn roots_follow_declaration_order() {
        let p = plan(
            r#"
cfg = config(name="fan-in")
roots = [bash_operator("r%d" % i, cmd="true", inputs=[], outputs=[]) for i in range(8)]
done = bash_operator("done", cmd="true", inputs=[], outputs=[], depends_on=roots)
build(cfg, done)
"#,
        );
        assert_eq!(
            ids(&p, p.roots()),
            ["r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7"]
        );
    }
}
