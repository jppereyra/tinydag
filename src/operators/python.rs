//! Python operator config and runtime.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::Operator;

/// Config for a Python script task.
///
/// `script` holds the path to the `.py` file. When compiled from a `.star`
/// file, it is resolved to an absolute path by `resolve`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PythonOperator {
    /// Path to the `.py` script file. Resolved to an absolute path at compile time.
    pub script: String,
    /// Named inputs this task reads. Order is not significant.
    pub inputs: Vec<String>,
    /// Named outputs this task produces. Order is not significant.
    pub outputs: Vec<String>,
}

impl Operator for PythonOperator {
    /// Resolve `script` to an absolute path relative to `base_dir`.
    ///
    /// Uses `canonicalize`, so the file must exist. Returns `Some(reason)` on
    /// failure, `None` on success.
    fn resolve(&mut self, base_dir: &Path) -> Option<String> {
        let joined = base_dir.join(&self.script);
        match joined.canonicalize() {
            Ok(abs) => {
                self.script = abs.to_string_lossy().into_owned();
                None
            }
            Err(e) => Some(format!("cannot resolve script '{}': {e}", joined.display())),
        }
    }

    /// Returns `Some(reason)` if this config fails validation.
    ///
    /// Runs `$TINYDAG_PYTHON -m py_compile` on the script.
    /// Path must already be absolute (resolve was called first).
    fn validate(&self) -> Option<String> {
        if self.script.trim().is_empty() {
            return Some("python script path is empty".to_string());
        }
        let python = std::env::var("TINYDAG_PYTHON").unwrap_or_else(|_| "python3".to_string());
        match std::process::Command::new(&python)
            .args(["-m", "py_compile"])
            .arg(Path::new(&self.script))
            .stderr(std::process::Stdio::piped())
            .output()
        {
            Ok(out) if out.status.success() => None,
            Ok(out) => Some(String::from_utf8_lossy(&out.stderr).trim().to_string()),
            Err(e) => Some(format!("failed to run {python} -m py_compile: {e}")),
        }
    }

    fn type_name(&self) -> &'static str {
        "python"
    }
}

/// Entry point for the `tinydag-op-python` binary.
pub fn run() -> ! {
    use std::fs;
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};
    use std::sync::atomic::Ordering;

    use super::{CHILD_PGID, FailedErrorType, OperatorFailure, read_outputs_file, run_operator};

    run_operator("python", |payload, work_dir| {
        let script = payload["task_ref"]["script"]
            .as_str()
            .ok_or_else(|| OperatorFailure {
                error_type: FailedErrorType::InvalidConfig,
                message: "missing script in python config".into(),
                exit_code: 1,
            })?;

        fs::write(
            work_dir.join("tinydag_inputs.json"),
            serde_json::to_string(&payload["inputs"]).unwrap(),
        )
        .map_err(io_err)?;
        fs::write(
            work_dir.join("tinydag_params.json"),
            serde_json::to_string(&payload["dag_params"]).unwrap(),
        )
        .map_err(io_err)?;

        let python = std::env::var("TINYDAG_PYTHON").unwrap_or_else(|_| "python3".to_string());

        let mut py = Command::new(&python);
        py.arg(script)
            .current_dir(work_dir)
            .stdout(Stdio::inherit())
            .env("TINYDAG_WORK_DIR", work_dir);

        let mut child = unsafe {
            py.pre_exec(|| {
                libc::setsid();
                Ok(())
            })
            .spawn()
            .map_err(|e| OperatorFailure {
                error_type: FailedErrorType::RuntimeError,
                message: format!("failed to spawn {python}: {e}"),
                exit_code: 1,
            })?
        };
        CHILD_PGID.store(child.id() as i32, Ordering::SeqCst);
        let status = child.wait().map_err(io_err)?;

        if !status.success() {
            let code = status.code().unwrap_or(-1);
            return Err(OperatorFailure {
                error_type: FailedErrorType::TaskFailed,
                message: format!("python exited {code}"),
                exit_code: code,
            });
        }
        read_outputs_file(work_dir)
    })
}

fn io_err(e: std::io::Error) -> super::OperatorFailure {
    super::OperatorFailure {
        error_type: super::FailedErrorType::RuntimeError,
        message: e.to_string(),
        exit_code: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // run() path — requires the full gRPC stack via LocalExecutor
    // -----------------------------------------------------------------------

    /// Write a temp `.py` script and return a handle that keeps the file alive.
    fn write_script(content: &str) -> tempfile::NamedTempFile {
        let file = tempfile::Builder::new()
            .suffix(".py")
            .tempfile()
            .expect("create temp python script");
        std::fs::write(file.path(), content).expect("write temp python script");
        file
    }

    async fn python_executor() -> crate::executor::LocalExecutor {
        use std::collections::HashMap;
        let test_bin = std::env::current_exe().expect("can't locate test binary");
        let binary = test_bin
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tinydag-op-python");
        let mut registry = HashMap::new();
        registry.insert("python".to_string(), binary.to_string_lossy().into_owned());
        crate::executor::LocalExecutor::with_registry(registry).await
    }

    fn python_payload(node_id: &str, script: &str) -> crate::executor::DispatchPayload {
        python_payload_with(node_id, script, Default::default(), Default::default())
    }

    fn python_payload_with(
        node_id: &str,
        script: &str,
        inputs: std::collections::HashMap<String, serde_json::Value>,
        dag_params: std::collections::HashMap<String, serde_json::Value>,
    ) -> crate::executor::DispatchPayload {
        use crate::dag::TaskRef;
        crate::executor::DispatchPayload {
            run_id: "run-1".to_string(),
            dag_id: "test-dag".to_string(),
            pipeline_id: "test-pipeline".to_string(),
            dag_version: "abc123".to_string(),
            team: "test-team".to_string(),
            user: "test-user".to_string(),
            trigger_type: "manual".to_string(),
            node_id: node_id.to_string(),
            task_ref: TaskRef::Python(PythonOperator {
                script: script.to_string(),
                inputs: vec![],
                outputs: vec![],
            }),
            inputs,
            dag_params,
            timeout_secs: None,
        }
    }

    #[tokio::test]
    async fn run_successful_returns_outputs() {
        use crate::executor::Executor as _;
        let script = write_script(
            "import json\nwith open('tinydag_outputs.json','w') as f: json.dump({'outputs':{'result':99}},f)\n",
        );
        let ex = python_executor().await;
        let outcome = ex
            .dispatch(python_payload("py-1", &script.path().to_string_lossy()))
            .await
            .unwrap();
        assert_eq!(outcome.exit_code, Some(0));
        assert_eq!(outcome.outputs["result"], serde_json::json!(99));
    }

    #[tokio::test]
    async fn run_empty_stdout_is_valid() {
        use crate::executor::Executor as _;
        let script = write_script("pass\n");
        let ex = python_executor().await;
        let outcome = ex
            .dispatch(python_payload("py-1", &script.path().to_string_lossy()))
            .await
            .unwrap();
        assert!(outcome.outputs.is_empty());
    }

    #[tokio::test]
    async fn run_non_zero_exit_returns_task_failed() {
        use crate::executor::{Executor as _, ExecutorError};
        let script = write_script("import sys; sys.exit(7)\n");
        let ex = python_executor().await;
        let err = ex
            .dispatch(python_payload("py-1", &script.path().to_string_lossy()))
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutorError::TaskFailed { code: 7, .. }));
    }

    #[tokio::test]
    async fn run_inputs_exposed_in_json_file() {
        use crate::executor::Executor as _;
        let script = write_script(
            "import json\nins=json.load(open('tinydag_inputs.json'))\nwith open('tinydag_outputs.json','w') as f: json.dump({'outputs':{'v':ins['my-key']}},f)\n",
        );
        let mut inputs = std::collections::HashMap::new();
        inputs.insert("my-key".to_string(), serde_json::json!("hello"));
        let ex = python_executor().await;
        let outcome = ex
            .dispatch(python_payload_with(
                "py-1",
                &script.path().to_string_lossy(),
                inputs,
                Default::default(),
            ))
            .await
            .unwrap();
        assert_eq!(outcome.outputs["v"], serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn run_params_exposed_in_json_file() {
        use crate::executor::Executor as _;
        let script = write_script(
            "import json\nparams=json.load(open('tinydag_params.json'))\nwith open('tinydag_outputs.json','w') as f: json.dump({'outputs':{'env':params['env']}},f)\n",
        );
        let mut params = std::collections::HashMap::new();
        params.insert("env".to_string(), serde_json::json!("prod"));
        let ex = python_executor().await;
        let outcome = ex
            .dispatch(python_payload_with(
                "py-1",
                &script.path().to_string_lossy(),
                Default::default(),
                params,
            ))
            .await
            .unwrap();
        assert_eq!(outcome.outputs["env"], serde_json::json!("prod"));
    }

    #[tokio::test]
    async fn run_missing_script_returns_task_failed() {
        use crate::executor::{Executor as _, ExecutorError};
        let ex = python_executor().await;
        let err = ex
            .dispatch(python_payload("py-1", "/nonexistent/tinydag_test_xyz.py"))
            .await
            .unwrap_err();
        assert!(matches!(err, ExecutorError::TaskFailed { .. }));
    }

    // -----------------------------------------------------------------------
    // Structural validation
    // -----------------------------------------------------------------------

    #[test]
    fn empty_script_is_invalid() {
        let cfg = PythonOperator {
            script: "".to_string(),
            inputs: vec![],
            outputs: vec![],
        };
        assert!(cfg.validate().is_some());
        let cfg_ws = PythonOperator {
            script: "   ".to_string(),
            inputs: vec![],
            outputs: vec![],
        };
        assert!(cfg_ws.validate().is_some());
    }

    // -----------------------------------------------------------------------
    // Subprocess syntax checks
    // -----------------------------------------------------------------------

    #[test]
    fn py_compile_passes_for_valid_script() {
        let file = tempfile::Builder::new().suffix(".py").tempfile().unwrap();
        std::fs::write(file.path(), "x = 1 + 2\nprint(x)\n").unwrap();
        let cfg = PythonOperator {
            script: file.path().to_string_lossy().into_owned(),
            inputs: vec![],
            outputs: vec![],
        };
        assert!(cfg.validate().is_none());
    }

    #[test]
    fn py_compile_catches_syntax_error() {
        let file = tempfile::Builder::new().suffix(".py").tempfile().unwrap();
        std::fs::write(file.path(), "def f(\n").unwrap();
        let cfg = PythonOperator {
            script: file.path().to_string_lossy().into_owned(),
            inputs: vec![],
            outputs: vec![],
        };
        assert!(cfg.validate().is_some(), "expected syntax error, got None");
    }

    #[test]
    fn py_compile_fails_for_missing_script() {
        let cfg = PythonOperator {
            script: "/tinydag_definitely_does_not_exist_xyz.py".to_string(),
            inputs: vec![],
            outputs: vec![],
        };
        assert!(cfg.validate().is_some());
    }

    // -----------------------------------------------------------------------
    // resolve
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_canonicalizes_to_absolute() {
        let dir = std::env::temp_dir();
        let file = tempfile::Builder::new()
            .suffix(".py")
            .tempfile_in(&dir)
            .unwrap();
        std::fs::write(file.path(), "x = 1\n").unwrap();
        let name = file
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let mut cfg = PythonOperator {
            script: name,
            inputs: vec![],
            outputs: vec![],
        };
        let err = cfg.resolve(&dir);
        assert!(err.is_none(), "resolve failed: {err:?}");
        assert!(cfg.script.starts_with('/'));
    }

    #[test]
    fn resolve_fails_for_nonexistent_script() {
        let dir = std::env::temp_dir();
        let mut cfg = PythonOperator {
            script: "tinydag_definitely_does_not_exist_xyz.py".to_string(),
            inputs: vec![],
            outputs: vec![],
        };
        assert!(cfg.resolve(&dir).is_some());
    }
}
