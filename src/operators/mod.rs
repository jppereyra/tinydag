use std::path::Path;

use allocative::Allocative;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use starlark::any::ProvidesStaticType;
use starlark::environment::GlobalsBuilder;
use starlark::values::list::ListRef;
use starlark::values::starlark_value;
use starlark::values::{
    AllocValue, Freeze, FreezeResult, Freezer, FrozenValue, Heap, NoSerialize, StarlarkValue,
    Trace, Value, ValueLike,
};

pub mod bash;
pub mod python;
mod runtime;

/// Interface every operator type must satisfy.
pub trait Operator {
    /// Returns `Some(reason)` if this operator fails validation.
    /// Assumes script paths are already absolute (resolve was called first).
    fn validate(&self) -> Option<String>;

    /// Resolve any script paths to absolute. Returns `Some(reason)` on failure.
    /// Default: no-op (for operators with no external scripts).
    fn resolve(&mut self, base_dir: &Path) -> Option<String> {
        let _ = base_dir;
        None
    }

    fn type_name(&self) -> &'static str;

    /// Returns bytes capturing this operator's runtime content for version hashing.
    /// Called after resolve(), so paths are already absolute.
    /// For inline commands: the command text. For script files: the file contents.
    fn content_for_hash(&self) -> Vec<u8> {
        Vec::new()
    }
}

/// How the task is invoked. A tagged union serialized with
/// `operator_type`: `{"operator_type": "<type>", <operator fields...>}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operator_type", rename_all = "snake_case")]
pub enum TaskRef {
    Python(python::PythonOperator),
    Bash(bash::BashOperator),
}

impl Operator for TaskRef {
    fn validate(&self) -> Option<String> {
        match self {
            TaskRef::Python(c) => c.validate(),
            TaskRef::Bash(c) => c.validate(),
        }
    }

    fn resolve(&mut self, base_dir: &Path) -> Option<String> {
        match self {
            TaskRef::Python(c) => c.resolve(base_dir),
            TaskRef::Bash(c) => c.resolve(base_dir),
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            TaskRef::Python(c) => c.type_name(),
            TaskRef::Bash(c) => c.type_name(),
        }
    }

    fn content_for_hash(&self) -> Vec<u8> {
        match self {
            TaskRef::Python(c) => c.content_for_hash(),
            TaskRef::Bash(c) => c.content_for_hash(),
        }
    }
}

#[cfg(test)]
impl TaskRef {
    /// Returns a minimal valid `TaskRef` for use in structural tests that don't
    /// care which operator type a node uses.
    pub(crate) fn stub() -> Self {
        TaskRef::Bash(bash::BashOperator {
            cmd: Some("echo ok".to_string()),
            script: None,
        })
    }
}

// ---------------------------------------------------------------------------
// TaskNode — Starlark value type returned by operator DSL functions
// ---------------------------------------------------------------------------

/// Unfrozen form of a task node value returned by operator DSL functions.
#[derive(Trace, Allocative, ProvidesStaticType, NoSerialize, Debug)]
pub struct TaskNode<'v> {
    pub task_id: String,
    #[trace(unsafe_ignore)]
    #[allocative(skip)]
    pub task_ref: TaskRef,
    pub depends_on: Vec<Value<'v>>,
    #[trace(unsafe_ignore)]
    pub max_attempts: u32,
    #[trace(unsafe_ignore)]
    pub delay_secs: u64,
    #[trace(unsafe_ignore)]
    pub timeout_secs: Option<u64>,
}

impl std::fmt::Display for TaskNode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<TaskNode {}>", self.task_id)
    }
}

#[starlark_value(type = "TaskNode")]
impl<'v> StarlarkValue<'v> for TaskNode<'v> {}

impl<'v> AllocValue<'v> for TaskNode<'v> {
    fn alloc_value(self, heap: &'v Heap) -> Value<'v> {
        heap.alloc_complex(self)
    }
}

impl<'v> Freeze for TaskNode<'v> {
    type Frozen = FrozenTaskNode;
    fn freeze(self, freezer: &Freezer) -> FreezeResult<FrozenTaskNode> {
        Ok(FrozenTaskNode {
            task_id: self.task_id,
            task_ref: self.task_ref,
            depends_on: self
                .depends_on
                .into_iter()
                .map(|v| freezer.freeze(v))
                .collect::<FreezeResult<_>>()?,
            max_attempts: self.max_attempts,
            delay_secs: self.delay_secs,
            timeout_secs: self.timeout_secs,
        })
    }
}

/// Frozen form of a task node (produced when the Starlark module is frozen).
#[derive(Allocative, ProvidesStaticType, Debug)]
pub struct FrozenTaskNode {
    pub task_id: String,
    #[allocative(skip)]
    pub task_ref: TaskRef,
    pub depends_on: Vec<FrozenValue>,
    pub max_attempts: u32,
    pub delay_secs: u64,
    pub timeout_secs: Option<u64>,
}

impl std::fmt::Display for FrozenTaskNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<TaskNode {}>", self.task_id)
    }
}

// Provide a no-op Serialize so erased_serde::Serialize is satisfied for StarlarkValue.
impl serde::Serialize for FrozenTaskNode {
    fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "FrozenTaskNode cannot be serialized",
        ))
    }
}

#[starlark_value(type = "TaskNode")]
impl<'v> StarlarkValue<'v> for FrozenTaskNode {
    type Canonical = TaskNode<'v>;
}

// ---------------------------------------------------------------------------
// Shared helpers used by operator DSL functions
// ---------------------------------------------------------------------------

/// Returns true if `v` is a `TaskNode` (live or frozen).
pub(crate) fn is_task_node(v: Value<'_>) -> bool {
    v.downcast_ref::<TaskNode<'_>>().is_some()
        || v.unpack_frozen()
            .and_then(|fv| fv.downcast_ref::<FrozenTaskNode>())
            .is_some()
}

/// Accepts `None`, a single `TaskNode`, or a list of `TaskNode` values.
/// Returns `Vec<Value<'v>>` (empty when `None`).
pub(crate) fn unpack_depends_on<'v>(v: Value<'v>) -> anyhow::Result<Vec<Value<'v>>> {
    if v.is_none() {
        return Ok(vec![]);
    }
    if is_task_node(v) {
        return Ok(vec![v]);
    }
    if let Some(list) = ListRef::from_value(v) {
        for item in list.iter() {
            if !is_task_node(item) {
                return Err(anyhow!(
                    "'depends_on' must contain task nodes, got {}",
                    item.get_type()
                ));
            }
        }
        return Ok(list.iter().collect());
    }
    Err(anyhow!(
        "'depends_on' must be None, a task node, or a list of task nodes, got {}",
        v.get_type()
    ))
}

/// Accepts `None` or a list of strings. Returns `Vec<String>` (empty when `None`).
pub(crate) fn unpack_string_list<'v>(v: Value<'v>, name: &str) -> anyhow::Result<Vec<String>> {
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
        "'{name}' must be None or a list of strings, got {}",
        v.get_type()
    ))
}

/// Accepts `None` or a positive integer. Returns `Option<u64>`.
pub(crate) fn unpack_optional_u64<'v>(v: Value<'v>, name: &str) -> anyhow::Result<Option<u64>> {
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

/// Accepts `None` or a string. Returns `Option<String>`.
pub(crate) fn unpack_optional_string<'v>(
    v: Value<'v>,
    name: &str,
) -> anyhow::Result<Option<String>> {
    if v.is_none() {
        return Ok(None);
    }
    if let Some(s) = v.unpack_str() {
        return Ok(Some(s.to_owned()));
    }
    Err(anyhow!(
        "'{name}' must be a string or None, got {}",
        v.get_type()
    ))
}

// ---------------------------------------------------------------------------
// Operator globals registry
// ---------------------------------------------------------------------------

/// Returns all operator DSL globals registration functions.
pub fn all_operator_globals() -> Vec<fn(&mut GlobalsBuilder)> {
    vec![bash::register_globals, python::register_globals]
}

pub use runtime::{CHILD_PGID, FailedErrorType, OperatorFailure, read_outputs_file, run_operator};
