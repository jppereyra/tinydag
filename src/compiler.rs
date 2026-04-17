//! Starlark-based pipeline DSL compiler.
//!
//! Parses `.star` files into a [`DagDef`].
//! Runs structural + validation checks at compile time and computes a
//! deterministic version hash on the resulting artifact.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::anyhow;
use starlark::any::ProvidesStaticType;
use starlark::environment::{Globals, GlobalsBuilder, Module};
use starlark::eval::Evaluator;
use starlark::starlark_module;
use starlark::syntax::{AstModule, Dialect};
use starlark::values::Value;
use starlark::values::dict::DictRef;
use starlark::values::list::ListRef;
use starlark::values::none::NoneType;

use crate::dag::{DagDef, Edge, Node, RetryPolicy, TaskRef, Trigger};
use crate::operators::{BashOperator, Operator as _, PythonOperator};

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct DagConfig {
    dag_id: String,
    pipeline_id: String,
    team: String,
    user: String,
    schedule: Option<String>,
    params: HashMap<String, serde_json::Value>,
}

struct OperatorDef {
    task_id: String,
    task_ref: TaskRef,
    retry: RetryPolicy,
    timeout_secs: Option<u64>,
}

// ---------------------------------------------------------------------------
// DagCollector — accumulates everything the DSL declares
// ---------------------------------------------------------------------------

#[derive(Default, ProvidesStaticType)]
struct DagCollector {
    dag_config: RefCell<Option<DagConfig>>,
    operators: RefCell<Vec<OperatorDef>>,
    edges: RefCell<Vec<(String, String)>>,
}

// ---------------------------------------------------------------------------
// Public error type
// ---------------------------------------------------------------------------

pub enum CompileError {
    Eval(anyhow::Error),
    Validation(Vec<crate::validation::ValidationError>),
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompileError::Eval(e) => write!(f, "{e}"),
            CompileError::Validation(errs) => {
                for e in errs {
                    writeln!(f, "{e}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::fmt::Debug for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<anyhow::Error> for CompileError {
    fn from(e: anyhow::Error) -> Self {
        CompileError::Eval(e)
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Accept a Starlark `None`, a single string, or a list of strings.
/// Returns an owned `Vec<String>` (empty when `None`).
fn unpack_string_or_list<'v>(v: Value<'v>, name: &str) -> anyhow::Result<Vec<String>> {
    if v.is_none() {
        return Ok(vec![]);
    }
    if let Some(s) = v.unpack_str() {
        return Ok(vec![s.to_owned()]);
    }
    unpack_string_list(v, name)
}

/// Accept a Starlark `None` or a list of strings.
/// Returns an owned `Vec<String>` (empty when `None`).
fn unpack_string_list<'v>(v: Value<'v>, name: &str) -> anyhow::Result<Vec<String>> {
    if v.is_none() {
        return Ok(vec![]);
    }
    if let Some(list) = ListRef::from_value(v) {
        let mut result = Vec::new();
        for item in list.iter() {
            match item.unpack_str() {
                Some(s) => result.push(s.to_owned()),
                None => {
                    return Err(anyhow!(
                        "'{name}' must be a list of strings, got non-string element"
                    ));
                }
            }
        }
        return Ok(result);
    }
    Err(anyhow!(
        "'{name}' must be a string or list of strings, got {}",
        v.get_type()
    ))
}

/// Accept a Starlark `None` or a positive integer; returns `Option<u64>`.
fn unpack_optional_u64<'v>(v: Value<'v>, name: &str) -> anyhow::Result<Option<u64>> {
    if v.is_none() {
        return Ok(None);
    }
    if let Some(i) = v.unpack_i32() {
        if i > 0 {
            return Ok(Some(i as u64));
        }
        return Err(anyhow!("'{name}' must be a positive integer, got {i}"));
    }
    Err(anyhow!("'{name}' must be an integer or None"))
}

/// Recursively convert a Starlark value to a `serde_json::Value`.
/// Supported types: None, bool, int (i32 range), string, list, dict with string keys.
fn starlark_to_json<'v>(v: Value<'v>) -> anyhow::Result<serde_json::Value> {
    if v.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if let Some(b) = v.unpack_bool() {
        return Ok(serde_json::Value::Bool(b));
    }
    if let Some(i) = v.unpack_i32() {
        return Ok(serde_json::json!(i));
    }
    if let Some(s) = v.unpack_str() {
        return Ok(serde_json::Value::String(s.to_owned()));
    }
    if let Some(list) = ListRef::from_value(v) {
        let items: anyhow::Result<Vec<_>> = list.iter().map(starlark_to_json).collect();
        return Ok(serde_json::Value::Array(items?));
    }
    if let Some(dict) = DictRef::from_value(v) {
        let mut map = serde_json::Map::new();
        for (k, val) in dict.iter() {
            let key = k
                .unpack_str()
                .ok_or_else(|| anyhow!("params dict keys must be strings"))?;
            map.insert(key.to_owned(), starlark_to_json(val)?);
        }
        return Ok(serde_json::Value::Object(map));
    }
    Err(anyhow!(
        "unsupported param value type '{}' in params dict",
        v.get_type()
    ))
}

/// Convert a Starlark `None` or dict-with-string-keys into a JSON params map.
fn unpack_params<'v>(v: Value<'v>) -> anyhow::Result<HashMap<String, serde_json::Value>> {
    if v.is_none() {
        return Ok(HashMap::new());
    }
    if let Some(dict) = DictRef::from_value(v) {
        let mut map = HashMap::new();
        for (k, val) in dict.iter() {
            let key = k
                .unpack_str()
                .ok_or_else(|| anyhow!("'params' dict keys must be strings"))?;
            map.insert(key.to_owned(), starlark_to_json(val)?);
        }
        return Ok(map);
    }
    Err(anyhow!("'params' must be a dict or None"))
}

// ---------------------------------------------------------------------------
// register_operator — shared by python_operator and bash_operator
// ---------------------------------------------------------------------------

fn register_operator(
    collector: &DagCollector,
    task_id: &str,
    task_ref: TaskRef,
    retry: RetryPolicy,
    timeout_secs: Option<u64>,
    depends_on_val: Value<'_>,
) -> anyhow::Result<String> {
    let depends_on = unpack_string_or_list(depends_on_val, "depends_on")?;

    // Validate against already-registered task IDs (forward references are errors).
    {
        let operators = collector.operators.borrow();
        let known_ids: HashSet<&str> = operators.iter().map(|op| op.task_id.as_str()).collect();

        if known_ids.contains(task_id) {
            return Err(anyhow!("duplicate task_id: '{task_id}'"));
        }

        for dep in depends_on.iter() {
            if !known_ids.contains(dep.as_str()) {
                return Err(anyhow!(
                    "task '{task_id}' depends_on unknown or forward-referenced task '{dep}'"
                ));
            }
        }
    }

    {
        let mut edges = collector.edges.borrow_mut();
        for dep in depends_on.iter() {
            edges.push((dep.clone(), task_id.to_owned()));
        }
    }

    {
        let mut operators = collector.operators.borrow_mut();
        operators.push(OperatorDef {
            task_id: task_id.to_owned(),
            task_ref,
            retry,
            timeout_secs,
        });
    }

    Ok(task_id.to_owned())
}

// ---------------------------------------------------------------------------
// DSL globals
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
#[starlark_module]
fn dag_globals(builder: &mut GlobalsBuilder) {
    /// Declare DAG metadata. Must be called exactly once per file.
    fn dag<'v>(
        dag_id: &str,
        #[starlark(require = named, default = "")] pipeline_id: &str,
        #[starlark(require = named, default = "")] team: &str,
        #[starlark(require = named, default = "")] user: &str,
        #[starlark(require = named, default = NoneType)] schedule: Value<'v>,
        #[starlark(require = named, default = NoneType)] params: Value<'v>,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<NoneType> {
        let collector = eval
            .extra
            .and_then(|e| e.downcast_ref::<DagCollector>())
            .ok_or_else(|| anyhow!("internal: DagCollector not set"))?;

        let mut cfg = collector.dag_config.borrow_mut();
        if cfg.is_some() {
            return Err(anyhow!("dag() was called more than once"));
        }

        let schedule = if schedule.is_none() {
            None
        } else if let Some(s) = schedule.unpack_str() {
            Some(s.to_owned())
        } else {
            return Err(anyhow!("'schedule' must be a string or None"));
        };

        let params_map = unpack_params(params)?;

        *cfg = Some(DagConfig {
            dag_id: dag_id.to_owned(),
            pipeline_id: pipeline_id.to_owned(),
            team: team.to_owned(),
            user: user.to_owned(),
            schedule,
            params: params_map,
        });

        Ok(NoneType)
    }

    /// Register a Python script task. Returns the task_id string so it can
    /// be passed as a `depends_on` argument to downstream tasks.
    fn python_operator<'v>(
        task_id: &str,
        #[starlark(require = named)] script: &str,
        #[starlark(require = named, default = NoneType)] inputs: Value<'v>,
        #[starlark(require = named, default = NoneType)] outputs: Value<'v>,
        #[starlark(require = named, default = NoneType)] depends_on: Value<'v>,
        #[starlark(require = named, default = NoneType)] timeout_secs: Value<'v>,
        #[starlark(require = named, default = 1i32)] max_attempts: i32,
        #[starlark(require = named, default = 0i32)] delay_secs: i32,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<String> {
        let collector = eval
            .extra
            .and_then(|e| e.downcast_ref::<DagCollector>())
            .ok_or_else(|| anyhow!("internal: DagCollector not set"))?;

        let inputs_vec = unpack_string_list(inputs, "inputs")?;
        let outputs_vec = unpack_string_list(outputs, "outputs")?;
        let timeout = unpack_optional_u64(timeout_secs, "timeout_secs")?;

        let task_ref = TaskRef::Python(PythonOperator {
            script: script.to_owned(),
            inputs: inputs_vec,
            outputs: outputs_vec,
        });
        let retry = RetryPolicy {
            max_attempts: max_attempts.max(1) as u32,
            delay_secs: delay_secs.max(0) as u64,
        };

        register_operator(collector, task_id, task_ref, retry, timeout, depends_on)
    }

    /// Register a Bash command task. Returns the task_id string so it can be
    /// passed as a `depends_on` argument to downstream tasks.
    fn bash_operator<'v>(
        task_id: &str,
        #[starlark(require = named, default = NoneType)] cmd: Value<'v>,
        #[starlark(require = named, default = NoneType)] script: Value<'v>,
        #[starlark(require = named, default = NoneType)] depends_on: Value<'v>,
        #[starlark(require = named, default = NoneType)] timeout_secs: Value<'v>,
        #[starlark(require = named, default = 1i32)] max_attempts: i32,
        #[starlark(require = named, default = 0i32)] delay_secs: i32,
        eval: &mut Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<String> {
        let collector = eval
            .extra
            .and_then(|e| e.downcast_ref::<DagCollector>())
            .ok_or_else(|| anyhow!("internal: DagCollector not set"))?;

        let timeout = unpack_optional_u64(timeout_secs, "timeout_secs")?;

        let cmd_opt = if cmd.is_none() {
            None
        } else {
            Some(
                cmd.unpack_str()
                    .ok_or_else(|| anyhow!("'cmd' must be a string"))?
                    .to_owned(),
            )
        };
        let script_opt = if script.is_none() {
            None
        } else {
            Some(
                script
                    .unpack_str()
                    .ok_or_else(|| anyhow!("'script' must be a string"))?
                    .to_owned(),
            )
        };

        match (&cmd_opt, &script_opt) {
            (None, None) => {
                return Err(anyhow!(
                    "bash_operator requires exactly one of 'cmd' or 'script'"
                ));
            }
            (Some(_), Some(_)) => {
                return Err(anyhow!(
                    "bash_operator accepts at most one of 'cmd' or 'script'"
                ));
            }
            _ => {}
        }

        let task_ref = TaskRef::Bash(BashOperator {
            cmd: cmd_opt,
            script: script_opt,
        });
        let retry = RetryPolicy {
            max_attempts: max_attempts.max(1) as u32,
            delay_secs: delay_secs.max(0) as u64,
        };

        register_operator(collector, task_id, task_ref, retry, timeout, depends_on)
    }
}

// ---------------------------------------------------------------------------
// compile_source — internal, called directly by tests
// ---------------------------------------------------------------------------

/// Run `ast` against `globals` with `collector` wired into `eval.extra`.
///
/// The only way to call `eval_module` in this codebase is through this
/// function, so `eval.extra` is always set — the "not set" branch in the DSL
/// functions cannot be reached.
fn eval_with_collector(
    ast: AstModule,
    globals: &Globals,
    collector: &DagCollector,
) -> anyhow::Result<()> {
    let module = Module::new();
    let mut eval = Evaluator::new(&module);
    eval.extra = Some(collector);
    eval.eval_module(ast, globals).map_err(|e| anyhow!("{e}"))?;
    Ok(())
}

pub(crate) fn compile_source(
    filename: &str,
    source: &str,
    base_dir: Option<&Path>,
) -> Result<DagDef, CompileError> {
    let ast = AstModule::parse(filename, source.to_owned(), &Dialect::Standard)
        .map_err(|e| anyhow!("{e}"))?;
    let globals = GlobalsBuilder::new().with(dag_globals).build();
    let collector = DagCollector::default();

    eval_with_collector(ast, &globals, &collector)?;

    let cfg = collector
        .dag_config
        .borrow_mut()
        .take()
        .ok_or_else(|| anyhow!("dag() was never called"))?;

    let trigger = cfg
        .schedule
        .map_or(Trigger::Manual, |s| Trigger::Cron { schedule: s });

    let nodes: Vec<Node> = collector
        .operators
        .into_inner()
        .into_iter()
        .map(|op| Node {
            id: op.task_id,
            task_ref: op.task_ref,
            retry: op.retry,
            timeout_secs: op.timeout_secs,
        })
        .collect();

    let edges: Vec<Edge> = collector
        .edges
        .into_inner()
        .into_iter()
        .map(|(from, to)| Edge { from, to })
        .collect();

    let mut dag = DagDef {
        version: 1,
        dag_id: cfg.dag_id,
        pipeline_id: cfg.pipeline_id,
        version_hash: String::new(),
        trigger,
        team: cfg.team,
        user: cfg.user,
        nodes,
        edges,
        metadata: HashMap::new(),
        params: cfg.params,
    };

    // Resolve pass: canonicalize script paths to absolute before validation.
    if let Some(dir) = base_dir {
        let mut resolve_errors: Vec<crate::validation::ValidationError> = Vec::new();
        for node in &mut dag.nodes {
            if let Some(reason) = node.task_ref.resolve(dir) {
                resolve_errors.push(crate::validation::ValidationError::InvalidTaskRef {
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
// Public entry point
// ---------------------------------------------------------------------------

/// Compile a `.star` pipeline file into a `DagDef`.
pub fn compile(path: &Path) -> Result<DagDef, CompileError> {
    let src = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("could not read '{}': {e}", path.display()))?;
    let base_dir = path.parent();
    compile_source(&path.to_string_lossy(), &src, base_dir)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SYNTAX_CHECK_SEQ: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn compile_minimal_bash_dag() {
        let src = r#"
dag("my-dag", pipeline_id="my-pipeline")
bash_operator("step1", cmd="echo hello")
"#;
        let dag = compile_source("test.star", src, None).unwrap();
        assert_eq!(dag.dag_id, "my-dag");
        assert_eq!(dag.nodes.len(), 1);
        assert_eq!(dag.nodes[0].id, "step1");
    }

    #[test]
    fn compile_trigger_defaults_to_manual() {
        let src = r#"
dag("d")
bash_operator("n", cmd="echo hi")
"#;
        let dag = compile_source("test.star", src, None).unwrap();
        assert_eq!(dag.trigger, Trigger::Manual);
    }

    #[test]
    fn compile_cron_trigger_when_schedule_set() {
        let src = r#"
dag("d", schedule="0 6 * * *")
bash_operator("n", cmd="echo hi")
"#;
        let dag = compile_source("test.star", src, None).unwrap();
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
dag("etl-sample", pipeline_id="sample-pipeline", team="data-eng", user="alice")
extract = bash_operator("extract", cmd="echo extracting")
transform_a = bash_operator("transform-a", cmd="echo a", depends_on=extract)
transform_b = bash_operator("transform-b", cmd="echo b", depends_on=extract)
bash_operator("load", cmd="echo loading", depends_on=[transform_a, transform_b])
"#;
        let dag = compile_source("test.star", src, None).unwrap();
        assert_eq!(dag.dag_id, "etl-sample");
        assert_eq!(dag.nodes.len(), 4);
        assert_eq!(dag.edges.len(), 4);
    }

    #[test]
    fn compile_depends_on_string_creates_edge() {
        let src = r#"
dag("d")
bash_operator("a", cmd="echo a")
bash_operator("b", cmd="echo b", depends_on="a")
"#;
        let dag = compile_source("test.star", src, None).unwrap();
        assert_eq!(dag.edges.len(), 1);
        assert_eq!(dag.edges[0].from, "a");
        assert_eq!(dag.edges[0].to, "b");
    }

    #[test]
    fn compile_depends_on_list_creates_edges() {
        let src = r#"
dag("d")
bash_operator("a", cmd="echo a")
bash_operator("b", cmd="echo b")
bash_operator("c", cmd="echo c", depends_on=["a", "b"])
"#;
        let dag = compile_source("test.star", src, None).unwrap();
        assert_eq!(dag.edges.len(), 2);
    }

    #[test]
    fn compile_python_operator_roundtrips_script() {
        // Python operator validate() runs py_compile, so we need a real file.
        let seq = SYNTAX_CHECK_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "tinydag_compiler_test_{}_{}.py",
            std::process::id(),
            seq
        ));
        std::fs::write(&path, "pass\n").unwrap();
        let script = path.to_string_lossy().into_owned();
        let src = format!("dag(\"d\")\npython_operator(\"step1\", script=\"{script}\")\n");
        let dag = compile_source("test.star", &src, None).unwrap();
        let _ = std::fs::remove_file(&path);
        match &dag.nodes[0].task_ref {
            TaskRef::Python(op) => assert_eq!(op.script, script),
            _ => panic!("expected Python operator"),
        }
    }

    #[test]
    fn compile_version_hash_is_set() {
        let src = r#"
dag("d")
bash_operator("n", cmd="echo hi")
"#;
        let dag = compile_source("test.star", src, None).unwrap();
        assert!(!dag.version_hash.is_empty());
        assert_ne!(dag.version_hash, "placeholder");
    }

    #[test]
    fn compile_error_dag_not_called() {
        let src = r#"
bash_operator("n", cmd="echo hi")
"#;
        assert!(compile_source("test.star", src, None).is_err());
    }

    #[test]
    fn compile_error_dag_called_twice() {
        let src = r#"
dag("d1")
dag("d2")
bash_operator("n", cmd="echo hi")
"#;
        assert!(compile_source("test.star", src, None).is_err());
    }

    #[test]
    fn compile_error_unknown_depends_on() {
        let src = r#"
dag("d")
bash_operator("b", cmd="echo b", depends_on="nonexistent")
"#;
        assert!(compile_source("test.star", src, None).is_err());
    }
}
