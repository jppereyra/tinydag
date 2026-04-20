use std::path::Path;

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
}

pub use bash::BashOperator;
pub use python::PythonOperator;
pub use runtime::{CHILD_PGID, FailedErrorType, OperatorFailure, read_outputs_file, run_operator};
