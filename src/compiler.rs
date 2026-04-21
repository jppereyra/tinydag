//! Starlark-based pipeline DSL compiler.
//!
//! Parses `.star` files into a [`DagDef`].
//! Runs structural + validation checks at compile time and computes a
//! deterministic version hash on the resulting artifact.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::Path;

use allocative::Allocative;
use anyhow::anyhow;
use starlark::any::ProvidesStaticType;
use starlark::environment::{GlobalsBuilder, Module};
use starlark::eval::Evaluator;
use starlark::starlark_module;
use starlark::starlark_simple_value;
use starlark::syntax::{AstModule, Dialect};
use starlark::values::dict::DictRef;
use starlark::values::list::ListRef;
use starlark::values::none::NoneType;
use starlark::values::starlark_value;
use starlark::values::{NoSerialize, StarlarkValue, Value, ValueLike};

use crate::dag::{DagDef, Edge, Node, RetryPolicy, Trigger};
use crate::operators::{FrozenTaskNode, Operator as _, TaskNode, is_task_node};
use crate::validation::ValidationError;
use starlark::values::FrozenValue;

// ---------------------------------------------------------------------------
// PipelineConfig — Starlark value returned by config()
// ---------------------------------------------------------------------------

#[derive(Debug, ProvidesStaticType, NoSerialize)]
struct PipelineConfig {
    dag_id: String,
    pipeline_id: String,
    team: String,
    user: String,
    schedule: Option<String>,
    params: HashMap<String, serde_json::Value>,
}

impl allocative::Allocative for PipelineConfig {
    fn visit<'a, 'b: 'a>(&self, visitor: &'a mut allocative::Visitor<'b>) {
        visitor.visit_simple_sized::<Self>();
    }
}

impl fmt::Display for PipelineConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<PipelineConfig {}>", self.dag_id)
    }
}

starlark_simple_value!(PipelineConfig);

#[starlark_value(type = "PipelineConfig")]
impl<'v> StarlarkValue<'v> for PipelineConfig {}

// ---------------------------------------------------------------------------
// BuiltDag — Starlark value returned by build()
// ---------------------------------------------------------------------------

#[derive(Debug, ProvidesStaticType, NoSerialize, Allocative)]
struct BuiltDag {
    #[allocative(skip)]
    dag_def: DagDef,
}

impl fmt::Display for BuiltDag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<BuiltDag {}>", self.dag_def.dag_id)
    }
}

starlark_simple_value!(BuiltDag);

#[starlark_value(type = "BuiltDag")]
impl<'v> StarlarkValue<'v> for BuiltDag {}

// ---------------------------------------------------------------------------
// CompileError
// ---------------------------------------------------------------------------

pub enum CompileError {
    Eval(anyhow::Error),
    Validation(Vec<crate::validation::ValidationError>),
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompileError::Eval(e) => write!(f, "{e}"),
            CompileError::Validation(errs) => {
                for (i, e) in errs.iter().enumerate() {
                    if i > 0 {
                        writeln!(f)?;
                    }
                    write!(f, "{e}")?;
                }
                Ok(())
            }
        }
    }
}

/// Returns the n-th backtick-quoted token in `s` (0-indexed).
fn nth_backtick(s: &str, n: usize) -> Option<&str> {
    let mut count = 0;
    let mut start: Option<usize> = None;
    for (i, c) in s.char_indices() {
        if c == '`' {
            if let Some(begin) = start.take() {
                if count == n {
                    return Some(&s[begin..i]);
                }
                count += 1;
            } else {
                start = Some(i + 1);
            }
        }
    }
    None
}

/// Rewrites Starlark's generic "Missing named-only parameter" error into a
/// user-friendly message with an example hint.
fn rewrite_eval_error(e: anyhow::Error) -> anyhow::Error {
    let msg = e.to_string();
    if !msg.contains("Missing named-only parameter") {
        return e;
    }
    if let (Some(param), Some(func)) = (nth_backtick(&msg, 0), nth_backtick(&msg, 1)) {
        let hint = match param {
            "outputs" => Some("list of output names, e.g. outputs=[\"result\"] or outputs=[]"),
            "inputs" => {
                Some("list of input names from predecessors, e.g. inputs=[\"data\"] or inputs=[]")
            }
            _ => crate::operators::param_hint(func, param),
        };
        if let Some(hint) = hint {
            let needle = format!("Missing named-only parameter `{param}` for call to `{func}`");
            return anyhow!(
                "{}",
                msg.replace(&needle, &format!("{func}: '{param}' is required ({hint})"))
            );
        }
    }
    anyhow!(
        "{}",
        msg.replace("Missing named-only parameter", "missing required parameter")
    )
}

impl fmt::Debug for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl From<anyhow::Error> for CompileError {
    fn from(e: anyhow::Error) -> Self {
        CompileError::Eval(e)
    }
}

// ---------------------------------------------------------------------------
// Starlark value helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Graph traversal for build()
// ---------------------------------------------------------------------------

/// Downcast a `Value` to a `TaskNode<'v>`, normalizing both live and frozen forms.
fn downcast_task_node<'v>(v: Value<'v>) -> anyhow::Result<TaskNode<'v>> {
    if let Some(n) = v.downcast_ref::<TaskNode<'v>>() {
        return Ok(TaskNode {
            task_id: n.task_id.clone(),
            task_ref: n.task_ref.clone(),
            depends_on: n.depends_on.clone(),
            inputs: n.inputs.clone(),
            outputs: n.outputs.clone(),
            max_attempts: n.max_attempts,
            delay_secs: n.delay_secs,
            timeout_secs: n.timeout_secs,
        });
    }
    if let Some(frozen) = v.unpack_frozen()
        && let Some(n) = frozen.downcast_ref::<FrozenTaskNode>()
    {
        return Ok(TaskNode {
            task_id: n.task_id.clone(),
            task_ref: n.task_ref.clone(),
            depends_on: n
                .depends_on
                .iter()
                .map(|fv: &FrozenValue| fv.to_value())
                .collect(),
            inputs: n.inputs.clone(),
            outputs: n.outputs.clone(),
            max_attempts: n.max_attempts,
            delay_secs: n.delay_secs,
            timeout_secs: n.timeout_secs,
        });
    }
    Err(anyhow!(
        "expected a task node (python_operator or bash_operator result), got {}",
        v.get_type()
    ))
}

/// Accepts a single `TaskNode` value or a list of `TaskNode` values.
fn unpack_task_node_list<'v>(v: Value<'v>) -> anyhow::Result<Vec<Value<'v>>> {
    if is_task_node(v) {
        return Ok(vec![v]);
    }
    if let Some(list) = ListRef::from_value(v) {
        for item in list.iter() {
            if !is_task_node(item) {
                return Err(anyhow!(
                    "build() terminals must be task nodes, got {}",
                    item.get_type()
                ));
            }
        }
        return Ok(list.iter().collect());
    }
    Err(anyhow!(
        "build() second argument must be a task node or list of task nodes, got {}",
        v.get_type()
    ))
}

/// BFS traversal from terminal nodes, collecting all reachable `Node`s and `Edge`s.
///
/// Duplicate `task_id` from two distinct `TaskNode` objects is an error.
/// The same object reachable via multiple paths (structural sharing in a diamond) is fine.
fn collect_graph<'v>(terminals: Vec<Value<'v>>) -> anyhow::Result<(Vec<Node>, Vec<Edge>)> {
    let mut seen: HashMap<String, Value<'v>> = HashMap::new();
    let mut nodes: Vec<Node> = Vec::new();
    let mut edges: Vec<Edge> = Vec::new();
    let mut queue: VecDeque<(Value<'v>, Option<String>)> = VecDeque::new();

    for t in terminals {
        queue.push_back((t, None));
    }

    while let Some((v, child_id)) = queue.pop_front() {
        let n = downcast_task_node(v)?;

        if let Some(cid) = child_id {
            edges.push(Edge {
                from: n.task_id.clone(),
                to: cid,
            });
        }

        if let Some(existing) = seen.get(&n.task_id) {
            if !existing.ptr_eq(v) {
                anyhow::bail!("duplicate task_id: '{}'", n.task_id);
            }
            continue; // same object reachable via multiple paths — OK
        }
        seen.insert(n.task_id.clone(), v);
        nodes.push(Node {
            id: n.task_id.clone(),
            task_ref: n.task_ref,
            retry: RetryPolicy {
                max_attempts: n.max_attempts,
                delay_secs: n.delay_secs,
            },
            timeout_secs: n.timeout_secs,
            inputs: n.inputs,
            outputs: n.outputs,
        });

        for dep in n.depends_on {
            queue.push_back((dep, Some(n.task_id.clone())));
        }
    }

    Ok((nodes, edges))
}

// ---------------------------------------------------------------------------
// DSL globals
// ---------------------------------------------------------------------------

#[starlark_module]
fn dag_compiler_globals(builder: &mut GlobalsBuilder) {
    /// Declare pipeline metadata. Returns a PipelineConfig value.
    fn config<'v>(
        name: &str,
        #[starlark(require = named, default = "")] pipeline_id: &str,
        #[starlark(require = named, default = "")] team: &str,
        #[starlark(require = named, default = "")] user: &str,
        #[starlark(require = named, default = NoneType)] schedule: Value<'v>,
        #[starlark(require = named, default = NoneType)] params: Value<'v>,
    ) -> anyhow::Result<PipelineConfig> {
        let schedule = if schedule.is_none() {
            None
        } else if let Some(s) = schedule.unpack_str() {
            Some(s.to_owned())
        } else {
            return Err(anyhow!("'schedule' must be a string or None"));
        };
        let params = if params.is_none() {
            HashMap::new()
        } else if DictRef::from_value(params).is_some() {
            let json = serde_json::to_value(params).map_err(|e| anyhow!("'params': {e}"))?;
            json.as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            return Err(anyhow!("'params' must be a dict or None"));
        };
        Ok(PipelineConfig {
            dag_id: name.to_owned(),
            pipeline_id: pipeline_id.to_owned(),
            team: team.to_owned(),
            user: user.to_owned(),
            schedule,
            params,
        })
    }

    /// Assemble a DAG from a config and terminal task nodes. Must be the last
    /// expression in the file (its return value is read by compile).
    fn build<'v>(cfg: Value<'v>, terminals: Value<'v>) -> anyhow::Result<BuiltDag> {
        let config = cfg.downcast_ref::<PipelineConfig>().ok_or_else(|| {
            anyhow!(
                "build() first argument must be a config() value, got {}",
                cfg.get_type()
            )
        })?;

        let terminal_list = unpack_task_node_list(terminals)?;
        let (nodes, edges) = collect_graph(terminal_list)?;

        let trigger = config
            .schedule
            .clone()
            .map_or(Trigger::Manual, |s| Trigger::Cron { schedule: s });

        Ok(BuiltDag {
            dag_def: DagDef {
                version: 1,
                dag_id: config.dag_id.clone(),
                pipeline_id: config.pipeline_id.clone(),
                version_hash: String::new(),
                trigger,
                team: config.team.clone(),
                user: config.user.clone(),
                nodes,
                edges,
                metadata: HashMap::new(),
                params: config.params.clone(),
            },
        })
    }
}

// ---------------------------------------------------------------------------
// compile
// ---------------------------------------------------------------------------

pub fn compile(
    filename: &str,
    source: &str,
    base_dir: Option<&Path>,
) -> Result<DagDef, CompileError> {
    let ast = AstModule::parse(filename, source.to_owned(), &Dialect::Standard)
        .map_err(|e| rewrite_eval_error(anyhow!("{e}")))?;

    let globals = {
        let mut builder = GlobalsBuilder::standard().with(dag_compiler_globals);
        for reg in crate::operators::all_operator_globals() {
            builder = builder.with(reg);
        }
        builder.build()
    };

    let module = Module::new();
    let return_val = {
        let mut eval = Evaluator::new(&module);
        eval.eval_module(ast, &globals)
            .map_err(|e| rewrite_eval_error(anyhow!("{e}")))?
    };

    let mut dag = {
        let built = return_val.downcast_ref::<BuiltDag>().ok_or_else(|| {
            if return_val.is_none() {
                anyhow!("build() was never called — the last expression must be build(cfg, ...)")
            } else {
                anyhow!(
                    "expected build() as the last expression, got a {} value",
                    return_val.get_type()
                )
            }
        })?;
        built.dag_def.clone()
    }; // borrow of return_val ends here

    // Resolve pass: canonicalize script paths before validation.
    if let Some(dir) = base_dir {
        let mut resolve_errors: Vec<ValidationError> = Vec::new();
        for node in &mut dag.nodes {
            if let Some(reason) = node.task_ref.resolve(dir) {
                resolve_errors.push(ValidationError::InvalidTaskRef {
                    node_id: node.id.clone(),
                    reason,
                });
            }
        }
        if !resolve_errors.is_empty() {
            return Err(CompileError::Validation(resolve_errors));
        }
    }

    crate::validation::validate(&dag).map_err(CompileError::Validation)?;
    dag.version_hash = dag.compute_version_hash();
    Ok(dag)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_minimal_bash_dag() {
        let src = r#"
cfg = config(name="my-dag", pipeline_id="my-pipeline")
n = bash_operator("step1", cmd="echo hello", inputs=[], outputs=[])
build(cfg, n)
"#;
        let dag = compile("test.star", src, None).unwrap();
        assert_eq!(dag.dag_id, "my-dag");
        assert_eq!(dag.nodes.len(), 1);
        assert_eq!(dag.nodes[0].id, "step1");
    }

    #[test]
    fn compile_trigger_defaults_to_manual() {
        let src = r#"
cfg = config(name="d")
n = bash_operator("n", cmd="echo hi", inputs=[], outputs=[])
build(cfg, n)
"#;
        let dag = compile("test.star", src, None).unwrap();
        assert_eq!(dag.trigger, Trigger::Manual);
    }

    #[test]
    fn compile_cron_trigger_when_schedule_set() {
        let src = r#"
cfg = config(name="d", schedule="0 6 * * *")
n = bash_operator("n", cmd="echo hi", inputs=[], outputs=[])
build(cfg, n)
"#;
        let dag = compile("test.star", src, None).unwrap();
        assert_eq!(
            dag.trigger,
            Trigger::Cron {
                schedule: "0 6 * * *".to_owned()
            }
        );
    }

    #[test]
    fn compile_diamond_matches_json() {
        let src = r#"
cfg = config(name="etl-sample", pipeline_id="sample-pipeline", team="data-eng", user="alice")
extract = bash_operator("extract", cmd="echo extracting", inputs=[], outputs=[])
transform_a = bash_operator("transform-a", cmd="echo a", inputs=[], outputs=[], depends_on=extract)
transform_b = bash_operator("transform-b", cmd="echo b", inputs=[], outputs=[], depends_on=extract)
loader = bash_operator("load", cmd="echo loading", inputs=[], outputs=[], depends_on=[transform_a, transform_b])
build(cfg, loader)
"#;
        let dag = compile("test.star", src, None).unwrap();
        assert_eq!(dag.dag_id, "etl-sample");
        assert_eq!(dag.nodes.len(), 4);
        assert_eq!(dag.edges.len(), 4);
    }

    #[test]
    fn compile_depends_on_node_creates_edge() {
        let src = r#"
cfg = config(name="d")
a = bash_operator("a", cmd="echo a", inputs=[], outputs=[])
b = bash_operator("b", cmd="echo b", inputs=[], outputs=[], depends_on=a)
build(cfg, b)
"#;
        let dag = compile("test.star", src, None).unwrap();
        assert_eq!(dag.edges.len(), 1);
        assert_eq!(dag.edges[0].from, "a");
        assert_eq!(dag.edges[0].to, "b");
    }

    #[test]
    fn compile_depends_on_list_creates_edges() {
        let src = r#"
cfg = config(name="d")
a = bash_operator("a", cmd="echo a", inputs=[], outputs=[])
b = bash_operator("b", cmd="echo b", inputs=[], outputs=[])
c = bash_operator("c", cmd="echo c", inputs=[], outputs=[], depends_on=[a, b])
build(cfg, c)
"#;
        let dag = compile("test.star", src, None).unwrap();
        assert_eq!(dag.edges.len(), 2);
    }

    #[test]
    fn compile_python_operator_roundtrips_script() {
        // Python operator validate() runs py_compile, so we need a real file.
        let file = tempfile::Builder::new().suffix(".py").tempfile().unwrap();
        std::fs::write(file.path(), "pass\n").unwrap();
        let script = file.path().to_string_lossy().into_owned();
        let src = format!(
            "cfg = config(name=\"d\")\ns = python_operator(\"step1\", script=\"{script}\", inputs=[], outputs=[])\nbuild(cfg, s)\n"
        );
        let dag = compile("test.star", &src, None).unwrap();
        let val = serde_json::to_value(&dag.nodes[0].task_ref).unwrap();
        assert_eq!(val["operator_type"], "python");
        assert_eq!(val["script"], script);
    }

    #[test]
    fn compile_version_hash_is_set() {
        let src = r#"
cfg = config(name="d")
n = bash_operator("n", cmd="echo hi", inputs=[], outputs=[])
build(cfg, n)
"#;
        let dag = compile("test.star", src, None).unwrap();
        assert!(!dag.version_hash.is_empty());
        assert_ne!(dag.version_hash, "placeholder");
    }

    #[test]
    fn compile_error_build_not_called() {
        // No build() call — last expression is None.
        let src = r#"
cfg = config(name="d")
bash_operator("n", cmd="echo hi", inputs=[], outputs=[])
"#;
        assert!(compile("test.star", src, None).is_err());
    }

    #[test]
    fn compile_error_depends_on_wrong_type() {
        // depends_on with a string instead of a task node value.
        let src = r#"
cfg = config(name="d")
a = bash_operator("a", cmd="echo a", inputs=[], outputs=[])
b = bash_operator("b", cmd="echo b", inputs=[], outputs=[], depends_on="a")
build(cfg, b)
"#;
        assert!(compile("test.star", src, None).is_err());
    }

    #[test]
    fn compile_diamond_shared_dep() {
        // a is a dependency of both b and c; node a must appear exactly once.
        let src = r#"
cfg = config(name="d")
a = bash_operator("a", cmd="echo a", inputs=[], outputs=[])
b = bash_operator("b", cmd="echo b", inputs=[], outputs=[], depends_on=a)
c = bash_operator("c", cmd="echo c", inputs=[], outputs=[], depends_on=a)
d = bash_operator("d", cmd="echo d", inputs=[], outputs=[], depends_on=[b, c])
build(cfg, d)
"#;
        let dag = compile("test.star", src, None).unwrap();
        assert_eq!(dag.nodes.len(), 4);
        assert_eq!(dag.edges.len(), 4);
    }

    #[test]
    fn compile_error_duplicate_task_id() {
        let src = r#"
cfg = config(name="d")
a1 = bash_operator("dup", cmd="echo a", inputs=[], outputs=[])
a2 = bash_operator("dup", cmd="echo b", inputs=[], outputs=[])
b = bash_operator("b", cmd="echo b", inputs=[], outputs=[], depends_on=[a1, a2])
build(cfg, b)
"#;
        assert!(compile("test.star", src, None).is_err());
    }

    #[test]
    fn compile_error_unsatisfied_input() {
        let src = r#"
cfg = config(name="d")
a = bash_operator("a", cmd="echo a", inputs=[], outputs=[])
b = bash_operator("b", cmd="echo b", inputs=["missing"], outputs=[], depends_on=a)
build(cfg, b)
"#;
        assert!(compile("test.star", src, None).is_err());
    }
}
