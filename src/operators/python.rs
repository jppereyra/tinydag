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

    use super::{parse_outputs_json, run_operator, scalar_str, FailedErrorType, CHILD_PGID};

    run_operator("python", |payload, work_dir| {
        let script = payload["task_ref"]["script"]
            .as_str()
            .ok_or_else(|| (FailedErrorType::Unspecified, "missing script in python config".into(), 1))?;

        let empty = serde_json::Map::new();
        let inputs = payload["inputs"].as_object().unwrap_or(&empty);
        let params = payload["dag_params"].as_object().unwrap_or(&empty);

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
        py.arg(script).current_dir(work_dir).stdout(Stdio::piped());
        for (k, v) in inputs {
            if let Some(s) = scalar_str(v) {
                py.env(format!("TINYDAG_INPUT_{}", k.to_uppercase().replace('-', "_")), s);
            }
        }
        for (k, v) in params {
            if let Some(s) = scalar_str(v) {
                py.env(format!("TINYDAG_PARAM_{}", k.to_uppercase().replace('-', "_")), s);
            }
        }

        let child = unsafe {
            py.pre_exec(|| {
                libc::setsid();
                Ok(())
            })
            .spawn()
            .map_err(|e| (FailedErrorType::Unspecified, format!("failed to spawn {python}: {e}"), 1))?
        };
        CHILD_PGID.store(child.id() as i32, Ordering::SeqCst);
        let output = child.wait_with_output().map_err(io_err)?;

        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            return Err((FailedErrorType::Unspecified, format!("python exited {code}"), code));
        }
        parse_outputs_json(&output.stdout)
    })
}

fn io_err(e: std::io::Error) -> (super::FailedErrorType, String, i32) {
    (super::FailedErrorType::Unspecified, e.to_string(), 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SYNTAX_CHECK_SEQ: AtomicU64 = AtomicU64::new(0);

    // -----------------------------------------------------------------------
    // run() path — requires the full gRPC stack via LocalExecutor
    // -----------------------------------------------------------------------

    /// Write a temp `.py` script and return its absolute path.
    fn write_script(content: &str) -> std::path::PathBuf {
        let seq = SYNTAX_CHECK_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir()
            .join(format!("tinydag_py_run_{}_{}.py", std::process::id(), seq));
        std::fs::write(&path, content).expect("write temp python script");
        path
    }

    async fn python_executor() -> crate::executor::LocalExecutor {
        use std::collections::HashMap;
        let test_bin = std::env::current_exe().expect("can't locate test binary");
        let binary = test_bin
            .parent().unwrap()
            .parent().unwrap()
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
            run_id:       "run-1".to_string(),
            dag_id:       "test-dag".to_string(),
            pipeline_id:  "test-pipeline".to_string(),
            dag_version:  "abc123".to_string(),
            team:         "test-team".to_string(),
            user:         "test-user".to_string(),
            trigger_type: "manual".to_string(),
            node_id:      node_id.to_string(),
            task_ref:     TaskRef::Python(PythonOperator {
                script:  script.to_string(),
                inputs:  vec![],
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
        let script = write_script("import json; print(json.dumps({'outputs':{'result':99}}))\n");
        let ex = python_executor().await;
        let outcome = ex.dispatch(python_payload("py-1", &script.to_string_lossy())).await.unwrap();
        assert_eq!(outcome.exit_code, Some(0));
        assert_eq!(outcome.outputs["result"], serde_json::json!(99));
    }

    #[tokio::test]
    async fn run_empty_stdout_is_valid() {
        use crate::executor::Executor as _;
        let script = write_script("pass\n");
        let ex = python_executor().await;
        let outcome = ex.dispatch(python_payload("py-1", &script.to_string_lossy())).await.unwrap();
        assert!(outcome.outputs.is_empty());
    }

    #[tokio::test]
    async fn run_non_zero_exit_returns_task_failed() {
        use crate::executor::{Executor as _, ExecutorError};
        let script = write_script("import sys; sys.exit(7)\n");
        let ex = python_executor().await;
        let err = ex.dispatch(python_payload("py-1", &script.to_string_lossy())).await.unwrap_err();
        assert!(matches!(err, ExecutorError::TaskFailed { code: 7, .. }));
    }

    #[tokio::test]
    async fn run_inputs_exposed_as_env_vars() {
        use crate::executor::Executor as _;
        let script = write_script(
            "import os, json\nprint(json.dumps({'outputs':{'v': os.environ['TINYDAG_INPUT_MY_KEY']}}))\n",
        );
        let mut inputs = std::collections::HashMap::new();
        inputs.insert("my-key".to_string(), serde_json::json!("hello"));
        let ex = python_executor().await;
        let outcome = ex
            .dispatch(python_payload_with("py-1", &script.to_string_lossy(), inputs, Default::default()))
            .await
            .unwrap();
        assert_eq!(outcome.outputs["v"], serde_json::json!("hello"));
    }

    #[tokio::test]
    async fn run_params_exposed_as_env_vars() {
        use crate::executor::Executor as _;
        let script = write_script(
            "import os, json\nprint(json.dumps({'outputs':{'env': os.environ['TINYDAG_PARAM_ENV']}}))\n",
        );
        let mut params = std::collections::HashMap::new();
        params.insert("env".to_string(), serde_json::json!("prod"));
        let ex = python_executor().await;
        let outcome = ex
            .dispatch(python_payload_with("py-1", &script.to_string_lossy(), Default::default(), params))
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
        let cfg = PythonOperator { script: "".to_string(), inputs: vec![], outputs: vec![] };
        assert!(cfg.validate().is_some());
        let cfg_ws = PythonOperator { script: "   ".to_string(), inputs: vec![], outputs: vec![] };
        assert!(cfg_ws.validate().is_some());
    }

    // -----------------------------------------------------------------------
    // Subprocess syntax checks
    // -----------------------------------------------------------------------

    #[test]
    fn py_compile_passes_for_valid_script() {
        let dir = std::env::temp_dir();
        let seq = SYNTAX_CHECK_SEQ.fetch_add(1, Ordering::Relaxed);
        let name = format!("tinydag_py_test_valid_{}_{}.py", std::process::id(), seq);
        let path = dir.join(&name);
        std::fs::write(&path, "x = 1 + 2\nprint(x)\n").unwrap();
        let cfg = PythonOperator { script: path.to_string_lossy().into_owned(), inputs: vec![], outputs: vec![] };
        let result = cfg.validate();
        let _ = std::fs::remove_file(&path);
        assert!(result.is_none());
    }

    #[test]
    fn py_compile_catches_syntax_error() {
        let dir = std::env::temp_dir();
        let seq = SYNTAX_CHECK_SEQ.fetch_add(1, Ordering::Relaxed);
        let name = format!("tinydag_py_test_bad_{}_{}.py", std::process::id(), seq);
        let path = dir.join(&name);
        std::fs::write(&path, "def f(\n").unwrap();
        let cfg = PythonOperator { script: path.to_string_lossy().into_owned(), inputs: vec![], outputs: vec![] };
        let result = cfg.validate();
        let _ = std::fs::remove_file(&path);
        assert!(result.is_some(), "expected syntax error, got None");
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
        let seq = SYNTAX_CHECK_SEQ.fetch_add(1, Ordering::Relaxed);
        let name = format!("tinydag_py_resolve_{}_{}.py", std::process::id(), seq);
        let path = dir.join(&name);
        std::fs::write(&path, "x = 1\n").unwrap();
        let mut cfg = PythonOperator { script: name, inputs: vec![], outputs: vec![] };
        let err = cfg.resolve(&dir);
        let _ = std::fs::remove_file(&path);
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
