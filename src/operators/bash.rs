//! Bash operator config and runtime.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::Operator;

/// Config for a Bash task.
///
/// Exactly one of `cmd` (inline shell command) or `script` (path to a `.sh`
/// file) must be set. When compiled from a `.star` file, `script` is resolved
/// to an absolute path by `resolve` before the DAG is persisted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BashOperator {
    /// Inline shell command.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cmd: Option<String>,
    /// Path to a `.sh` script file. Resolved to an absolute path at compile time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
}

impl Operator for BashOperator {
    /// Resolve `script` (if set) to an absolute path relative to `base_dir`.
    ///
    /// Uses `canonicalize`, so the file must exist. Returns `Some(reason)` on
    /// failure, `None` on success (or when `script` is not set).
    fn resolve(&mut self, base_dir: &Path) -> Option<String> {
        let script = self.script.as_deref()?;
        let joined = base_dir.join(script);
        match joined.canonicalize() {
            Ok(abs) => {
                self.script = Some(abs.to_string_lossy().into_owned());
                None
            }
            Err(e) => Some(format!("cannot resolve script '{}': {e}", joined.display())),
        }
    }

    /// Returns `Some(reason)` if this config fails validation.
    ///
    /// Structural check: exactly one of `cmd` / `script` must be set and
    /// must be non-empty.
    ///
    /// Subprocess check: runs `bash -n` on the command (via a temp file) or
    /// on the script file directly (path must already be absolute).
    fn validate(&self) -> Option<String> {
        match (&self.cmd, &self.script) {
            (None, None) => {
                Some("bash_operator requires exactly one of 'cmd' or 'script'".to_string())
            }
            (Some(_), Some(_)) => {
                Some("bash_operator accepts at most one of 'cmd' or 'script'".to_string())
            }
            (Some(cmd), None) => {
                if cmd.trim().is_empty() {
                    return Some("bash cmd is empty".to_string());
                }
                bash_n_cmd(cmd)
            }
            (None, Some(script)) => {
                if script.trim().is_empty() {
                    return Some("bash script path is empty".to_string());
                }
                bash_n_file(Path::new(script))
            }
        }
    }

    fn type_name(&self) -> &'static str {
        "bash"
    }
}

fn bash_n_cmd(cmd: &str) -> Option<String> {
    let tmp = match tempfile::Builder::new().suffix(".sh").tempfile() {
        Ok(f) => f,
        Err(e) => return Some(format!("failed to create syntax check file: {e}")),
    };
    if let Err(e) = std::fs::write(tmp.path(), cmd) {
        return Some(format!("failed to write syntax check file: {e}"));
    }
    bash_n_file(tmp.path())
}

fn bash_n_file(path: &Path) -> Option<String> {
    match std::process::Command::new("bash")
        .arg("-n")
        .arg(path)
        .stderr(std::process::Stdio::piped())
        .output()
    {
        Ok(out) if out.status.success() => None,
        Ok(out) => Some(String::from_utf8_lossy(&out.stderr).trim().to_string()),
        Err(e) => Some(format!("failed to run bash -n: {e}")),
    }
}

/// Entry point for the `tinydag-op-bash` binary.
pub fn run() -> ! {
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use std::os::unix::process::CommandExt as _;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::sync::atomic::Ordering;

    use super::{CHILD_PGID, FailedErrorType, OperatorFailure, read_outputs_file, run_operator};

    run_operator("bash", |payload, work_dir| {
        let cmd_val = payload["task_ref"]["cmd"].as_str();
        let script_val = payload["task_ref"]["script"].as_str();

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

        // Determine the script to run.
        let script_path: PathBuf = if let Some(cmd) = cmd_val {
            let p = work_dir.join("script.sh");
            fs::write(&p, cmd).map_err(io_err)?;
            fs::set_permissions(&p, fs::Permissions::from_mode(0o755)).map_err(io_err)?;
            p
        } else if let Some(script) = script_val {
            PathBuf::from(script)
        } else {
            return Err(OperatorFailure {
                error_type: FailedErrorType::InvalidConfig,
                message: "bash config: neither cmd nor script is set".into(),
                exit_code: 1,
            });
        };

        let mut bash = Command::new("bash");
        bash.arg(&script_path)
            .current_dir(work_dir)
            .stdout(Stdio::inherit())
            .env("TINYDAG_WORK_DIR", work_dir);

        let mut child = unsafe {
            bash.pre_exec(|| {
                libc::setsid();
                Ok(())
            })
            .spawn()
            .map_err(io_err)?
        };
        CHILD_PGID.store(child.id() as i32, Ordering::SeqCst);
        let status = child.wait().map_err(io_err)?;

        if !status.success() {
            let code = status.code().unwrap_or(-1);
            return Err(OperatorFailure {
                error_type: FailedErrorType::TaskFailed,
                message: format!("bash exited {code}"),
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
    // Structural validation
    // -----------------------------------------------------------------------

    #[test]
    fn neither_cmd_nor_script_is_invalid() {
        let cfg = BashOperator {
            cmd: None,
            script: None,
        };
        assert!(cfg.validate().is_some());
    }

    #[test]
    fn both_cmd_and_script_is_invalid() {
        let cfg = BashOperator {
            cmd: Some("echo hi".to_string()),
            script: Some("run.sh".to_string()),
        };
        assert!(cfg.validate().is_some());
    }

    #[test]
    fn empty_cmd_is_invalid() {
        let cfg = BashOperator {
            cmd: Some("".to_string()),
            script: None,
        };
        assert!(cfg.validate().is_some());
        let cfg_ws = BashOperator {
            cmd: Some("   ".to_string()),
            script: None,
        };
        assert!(cfg_ws.validate().is_some());
    }

    #[test]
    fn empty_script_is_invalid() {
        let cfg = BashOperator {
            cmd: None,
            script: Some("".to_string()),
        };
        assert!(cfg.validate().is_some());
    }

    // -----------------------------------------------------------------------
    // Subprocess syntax checks
    // -----------------------------------------------------------------------

    #[test]
    fn bash_n_passes_for_valid_cmd() {
        let cfg = BashOperator {
            cmd: Some("echo hello".to_string()),
            script: None,
        };
        assert!(cfg.validate().is_none());
    }

    #[test]
    fn bash_n_catches_syntax_error_in_cmd() {
        let cfg = BashOperator {
            cmd: Some("if then done".to_string()),
            script: None,
        };
        assert!(cfg.validate().is_some());
    }

    #[test]
    fn bash_n_passes_for_valid_script_file() {
        let file = tempfile::Builder::new().suffix(".sh").tempfile().unwrap();
        std::fs::write(file.path(), "echo hello\n").unwrap();
        let cfg = BashOperator {
            cmd: None,
            script: Some(file.path().to_string_lossy().into_owned()),
        };
        assert!(cfg.validate().is_none());
    }

    #[test]
    fn bash_n_catches_syntax_error_in_script_file() {
        let file = tempfile::Builder::new().suffix(".sh").tempfile().unwrap();
        std::fs::write(file.path(), "if then done\n").unwrap();
        let cfg = BashOperator {
            cmd: None,
            script: Some(file.path().to_string_lossy().into_owned()),
        };
        assert!(cfg.validate().is_some());
    }

    // -----------------------------------------------------------------------
    // resolve
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_is_no_op_for_cmd() {
        let mut cfg = BashOperator {
            cmd: Some("echo hi".to_string()),
            script: None,
        };
        assert!(cfg.resolve(Path::new("/tmp")).is_none());
        assert_eq!(cfg.cmd, Some("echo hi".to_string()));
        assert!(cfg.script.is_none());
    }

    #[test]
    fn resolve_canonicalizes_script_to_absolute() {
        let dir = std::env::temp_dir();
        let file = tempfile::Builder::new()
            .suffix(".sh")
            .tempfile_in(&dir)
            .unwrap();
        std::fs::write(file.path(), "echo hi\n").unwrap();
        let name = file
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let mut cfg = BashOperator {
            cmd: None,
            script: Some(name),
        };
        let err = cfg.resolve(&dir);
        assert!(err.is_none(), "resolve failed: {err:?}");
        assert!(cfg.script.as_deref().unwrap().starts_with('/'));
    }

    #[test]
    fn resolve_fails_for_nonexistent_script() {
        let dir = std::env::temp_dir();
        let mut cfg = BashOperator {
            cmd: None,
            script: Some("tinydag_definitely_does_not_exist_xyz.sh".to_string()),
        };
        assert!(cfg.resolve(&dir).is_some());
    }
}
