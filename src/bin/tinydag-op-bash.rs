//! tinydag bash operator.
//!
//! Reads a JSON dispatch payload from stdin, writes the command to a temporary
//! script file, and runs it via `bash` in a fresh temporary working directory.
//!
//! ## gRPC protocol (when TINYDAG_CONTROL_ENDPOINT is set)
//!
//! 1. Connect to the tinydag gRPC control server.
//! 2. Spawn bash with captured stdout.
//! 3. Call ReportStarted.
//! 4. Heartbeat thread sends ReportHeartbeat every 10 s.
//! 5. On bash exit: call ReportSucceeded (exit 0, valid JSON stdout) or
//!    ReportFailed (non-zero exit or invalid JSON). Exit 0 regardless.
//!
//! ## Legacy protocol (fallback when TINYDAG_CONTROL_ENDPOINT is absent)
//!
//! stdout/stderr flow through unchanged. Exit code mirrors the script's exit code.
//!
//! ## Signal handling
//!
//! On SIGTERM the operator kills the script's entire process group before
//! exiting, so subprocesses spawned by the script don't become orphans.

use std::fs;
use std::io::{self, Read};
use std::os::unix::process::CommandExt as _;
use std::process::{self, Command, Stdio};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::transport::Channel;

mod proto {
    tonic::include_proto!("operator_control");
}

use proto::operator_control_client::OperatorControlClient;
use proto::{FailedErrorType, FailedEvent, HeartbeatEvent, StartedEvent, SucceededEvent};

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

// PGID of the script child, written before we block on wait().
// Read by the SIGTERM handler to kill the whole process group.
static CHILD_PGID: AtomicI32 = AtomicI32::new(0);

extern "C" fn handle_sigterm(_: libc::c_int) {
    let pgid = CHILD_PGID.load(Ordering::SeqCst);
    if pgid > 0 {
        // SAFETY: async-signal-safe; pgid is a process group we own.
        unsafe { libc::killpg(pgid as libc::pid_t, libc::SIGTERM) };
    }
    // _exit is async-signal-safe; exit is not.
    unsafe { libc::_exit(128 + libc::SIGTERM) };
}

// ---------------------------------------------------------------------------
// gRPC session
// ---------------------------------------------------------------------------

/// Active gRPC session for one task dispatch.
struct GrpcSession {
    rt:               Arc<tokio::runtime::Runtime>,
    client:           OperatorControlClient<Channel>,
    run_id:           String,
    task_id:          String,
    heartbeat_stop:   mpsc::SyncSender<()>,
    heartbeat_thread: Option<std::thread::JoinHandle<()>>,
}

impl GrpcSession {
    /// Connect to `endpoint`, call ReportStarted, and launch the heartbeat thread.
    fn connect_and_start(
        endpoint: String,
        run_id: String,
        task_id: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?,
        );

        let mut client = rt.block_on(async { OperatorControlClient::connect(endpoint).await })?;

        rt.block_on(async {
            client
                .report_started(StartedEvent {
                    run_id: run_id.clone(),
                    task_id: task_id.clone(),
                })
                .await
        })?;

        // Heartbeat thread: sends a heartbeat every 10 s until signalled.
        let (stop_tx, stop_rx) = mpsc::sync_channel::<()>(1);
        let rt2 = Arc::clone(&rt);
        let mut hb_client = client.clone();
        let hb_run_id = run_id.clone();
        let hb_task_id = task_id.clone();

        let heartbeat_thread = std::thread::spawn(move || loop {
            match stop_rx.recv_timeout(Duration::from_secs(30)) {
                Ok(_) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let ts = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    let _ = rt2.block_on(async {
                        hb_client
                            .report_heartbeat(HeartbeatEvent {
                                run_id: hb_run_id.clone(),
                                task_id: hb_task_id.clone(),
                                timestamp_unix_secs: ts,
                            })
                            .await
                    });
                }
            }
        });

        Ok(GrpcSession {
            rt,
            client,
            run_id,
            task_id,
            heartbeat_stop: stop_tx,
            heartbeat_thread: Some(heartbeat_thread),
        })
    }

    /// Stop the heartbeat thread. Must be called before reporting the result.
    fn stop_heartbeat(&mut self) {
        let _ = self.heartbeat_stop.try_send(());
        if let Some(t) = self.heartbeat_thread.take() {
            let _ = t.join();
        }
    }

    fn report_succeeded(&mut self, outputs_json: String) {
        let _ = self.rt.block_on(async {
            self.client
                .report_succeeded(SucceededEvent {
                    run_id: self.run_id.clone(),
                    task_id: self.task_id.clone(),
                    outputs_json,
                })
                .await
        });
    }

    fn report_failed(&mut self, error_type: FailedErrorType, message: String, exit_code: i32) {
        let _ = self.rt.block_on(async {
            self.client
                .report_failed(FailedEvent {
                    run_id: self.run_id.clone(),
                    task_id: self.task_id.clone(),
                    error_type: error_type as i32,
                    message,
                    exit_code,
                })
                .await
        });
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    // Register SIGTERM handler before spawning anything.
    unsafe {
        libc::signal(libc::SIGTERM, handle_sigterm as *const () as libc::sighandler_t);
    }

    let mut stdin_buf = String::new();
    if let Err(e) = io::stdin().read_to_string(&mut stdin_buf) {
        eprintln!("tinydag-op-bash: failed to read stdin: {e}");
        process::exit(1);
    }

    let payload: serde_json::Value = match serde_json::from_str(&stdin_buf) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("tinydag-op-bash: failed to parse dispatch payload: {e}");
            process::exit(1);
        }
    };

    let cmd = match payload["task_ref"]["config"]["cmd"].as_str() {
        Some(s) => s.to_string(),
        None => {
            eprintln!("tinydag-op-bash: missing task_ref.config.cmd in payload");
            process::exit(1);
        }
    };

    let node_id = payload["node_id"].as_str().unwrap_or("task");
    let run_id  = payload["run_id"].as_str().unwrap_or("");

    // Fresh working directory per task run.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let work_dir = std::env::temp_dir()
        .join(format!("tinydag-work-{node_id}-{}-{nanos}", process::id()));

    if let Err(e) = fs::create_dir_all(&work_dir) {
        eprintln!("tinydag-op-bash: failed to create work dir: {e}");
        process::exit(1);
    }

    // Write cmd to a script file so multi-line commands and here-docs work.
    let script = work_dir.join("script.sh");
    if let Err(e) = fs::write(&script, &cmd) {
        let _ = fs::remove_dir_all(&work_dir);
        eprintln!("tinydag-op-bash: failed to write script: {e}");
        process::exit(1);
    }

    use std::os::unix::fs::PermissionsExt as _;
    let _ = fs::set_permissions(&script, fs::Permissions::from_mode(0o755));

    // Check at runtime whether the gRPC control server endpoint is provided.
    let grpc_endpoint = std::env::var("TINYDAG_CONTROL_ENDPOINT").ok();

    // When using gRPC we capture bash stdout to extract the outputs JSON.
    // In the legacy fallback we pass stdout through unchanged.
    let bash_stdout = if grpc_endpoint.is_some() { Stdio::piped() } else { Stdio::inherit() };

    // Spawn bash in the work dir.
    // pre_exec runs in the child after fork but before exec:
    //   setsid() makes the child a session leader with a new PGID == its PID,
    //   so killpg() in the signal handler reaches the whole subprocess tree.
    let child = unsafe {
        Command::new("bash")
            .arg(&script)
            .current_dir(&work_dir)
            .stdout(bash_stdout)
            .pre_exec(|| {
                libc::setsid();
                Ok(())
            })
            .spawn()
    }
    .unwrap_or_else(|e| {
        let _ = fs::remove_dir_all(&work_dir);
        eprintln!("tinydag-op-bash: failed to spawn bash: {e}");
        process::exit(1);
    });

    // After setsid() the child's PGID equals its PID.
    CHILD_PGID.store(i32::try_from(child.id()).unwrap_or(0), Ordering::SeqCst);

    // Start gRPC session (ReportStarted + heartbeat thread) if endpoint is set.
    let mut session = grpc_endpoint.as_ref().map(|endpoint| {
        GrpcSession::connect_and_start(
            endpoint.clone(),
            run_id.to_string(),
            node_id.to_string(),
        )
        .unwrap_or_else(|e| {
            eprintln!("tinydag-op-bash: failed to connect to control server: {e}");
            process::exit(1);
        })
    });

    // Wait for bash to finish, collecting stdout for output extraction.
    let output = child.wait_with_output().unwrap_or_else(|e| {
        let _ = fs::remove_dir_all(&work_dir);
        eprintln!("tinydag-op-bash: failed to wait for child: {e}");
        process::exit(1);
    });

    let _ = fs::remove_dir_all(&work_dir);

    // --- gRPC result reporting ---
    if let Some(ref mut session) = session {
        session.stop_heartbeat();

        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            session.report_failed(FailedErrorType::Unspecified, format!("bash exited with code {code}"), code);
            process::exit(0);
        }

        // Parse the bash script's stdout as JSON outputs.
        let stdout_str = String::from_utf8_lossy(&output.stdout);
        let outputs_json = if stdout_str.trim().is_empty() {
            "{}".to_string()
        } else {
            match serde_json::from_str::<serde_json::Value>(&stdout_str) {
                Ok(v) => match v.get("outputs") {
                    Some(outputs) => {
                        serde_json::to_string(outputs).unwrap_or_else(|_| "{}".to_string())
                    }
                    None => {
                        session.report_failed(
                            FailedErrorType::InvalidOutput,
                            "bash stdout did not contain an \"outputs\" key".to_string(),
                            0,
                        );
                        process::exit(0);
                    }
                },
                Err(e) => {
                    session.report_failed(
                        FailedErrorType::InvalidOutput,
                        format!("expected {{\"outputs\": {{...}}}} from bash stdout: {e}"),
                        0,
                    );
                    process::exit(0);
                }
            }
        };

        session.report_succeeded(outputs_json);
        process::exit(0);
    }

    // --- Legacy fallback (no TINYDAG_CONTROL_ENDPOINT) ---
    // stdout already flowed through (Stdio::inherit); just mirror the exit code.
    process::exit(output.status.code().unwrap_or(1));
}
