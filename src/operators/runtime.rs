//! Shared operator library.
//!
//! Provides the `run_operator` entry-point used by all operators. Handles stdin
//! parsing, work-dir management, signal forwarding, and the gRPC result protocol.

use std::io::Read as _;
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::transport::Channel;

mod proto {
    tonic::include_proto!("operator_control");
}

use proto::operator_control_client::OperatorControlClient;
use proto::{FailedEvent, HeartbeatEvent, StartedEvent, SucceededEvent};

pub use proto::FailedErrorType;

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

/// PGID of the child process. Set by operators after spawning. Read by the
/// SIGTERM handler to kill the whole process group.
pub static CHILD_PGID: AtomicI32 = AtomicI32::new(0);

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
// gRPC session (private)
// ---------------------------------------------------------------------------

struct GrpcSession {
    rt: Arc<tokio::runtime::Runtime>,
    client: OperatorControlClient<Channel>,
    run_id: String,
    task_id: String,
    heartbeat_stop: mpsc::SyncSender<()>,
    heartbeat_thread: Option<std::thread::JoinHandle<()>>,
}

impl GrpcSession {
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

        let (stop_tx, stop_rx) = mpsc::sync_channel::<()>(1);
        let rt2 = Arc::clone(&rt);
        let mut hb_client = client.clone();
        let hb_run_id = run_id.clone();
        let hb_task_id = task_id.clone();

        let heartbeat_thread = std::thread::spawn(move || {
            loop {
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
// Public helpers
// ---------------------------------------------------------------------------

/// Read `tinydag_outputs.json` from the operator's work directory.
///
/// File absent → `Ok("{}")`. File present with valid `{"outputs": {...}}` →
/// `Ok(serialized_inner_map)`. File present but invalid → `Err(InvalidOutput, ...)`.
pub fn read_outputs_file(work_dir: &Path) -> Result<String, (FailedErrorType, String, i32)> {
    let path = work_dir.join("tinydag_outputs.json");
    let contents = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok("{}".to_string()),
        Err(e) => {
            return Err((
                FailedErrorType::InvalidOutput,
                format!("failed to read tinydag_outputs.json: {e}"),
                0,
            ));
        }
    };
    if contents.trim().is_empty() {
        return Ok("{}".to_string());
    }
    match serde_json::from_str::<serde_json::Value>(&contents) {
        Ok(v) => match v.get("outputs") {
            Some(outputs) => {
                Ok(serde_json::to_string(outputs).unwrap_or_else(|_| "{}".to_string()))
            }
            None => Err((
                FailedErrorType::InvalidOutput,
                "tinydag_outputs.json did not contain an \"outputs\" key".to_string(),
                0,
            )),
        },
        Err(e) => Err((
            FailedErrorType::InvalidOutput,
            format!("expected {{\"outputs\": {{...}}}} in tinydag_outputs.json: {e}"),
            0,
        )),
    }
}

// ---------------------------------------------------------------------------
// run_operator
// ---------------------------------------------------------------------------

/// Run an operator: handle stdin, work dir, signals, and the gRPC result protocol.
///
/// The closure receives `(&payload, &work_dir)` and returns:
/// - `Ok(outputs_json_string)` — the inner map, e.g. `r#"{"rows":42}"#`
/// - `Err((error_type, message, exit_code))`
///
/// `TINYDAG_CONTROL_ENDPOINT` must be set; if absent the operator exits with
/// an error. This function never returns (`-> !`).
pub fn run_operator<F>(operator_name: &'static str, f: F) -> !
where
    F: FnOnce(&serde_json::Value, &Path) -> Result<String, (FailedErrorType, String, i32)>,
{
    // 1. Register SIGTERM handler.
    unsafe {
        libc::signal(
            libc::SIGTERM,
            handle_sigterm as *const () as libc::sighandler_t,
        );
    }

    // 2. Read + parse stdin.
    let mut stdin_buf = String::new();
    if let Err(e) = std::io::stdin().read_to_string(&mut stdin_buf) {
        eprintln!("{operator_name}: failed to read stdin: {e}");
        std::process::exit(1);
    }
    let payload: serde_json::Value = match serde_json::from_str(&stdin_buf) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{operator_name}: failed to parse dispatch payload: {e}");
            std::process::exit(1);
        }
    };

    // 3. Extract node_id, run_id.
    let node_id = payload["node_id"].as_str().unwrap_or("task").to_string();
    let run_id = payload["run_id"].as_str().unwrap_or("").to_string();

    // 4. Create work dir.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let work_dir = std::env::temp_dir().join(format!(
        "tinydag-work-{node_id}-{}-{nanos}",
        std::process::id()
    ));
    if let Err(e) = std::fs::create_dir_all(&work_dir) {
        eprintln!("{operator_name}: failed to create work dir: {e}");
        std::process::exit(1);
    }

    // 5. Require TINYDAG_CONTROL_ENDPOINT.
    let endpoint = match std::env::var("TINYDAG_CONTROL_ENDPOINT") {
        Ok(v) => v,
        Err(_) => {
            let _ = std::fs::remove_dir_all(&work_dir);
            eprintln!("{operator_name}: TINYDAG_CONTROL_ENDPOINT is not set");
            std::process::exit(1);
        }
    };

    // 6. Connect and report started.
    let mut session = GrpcSession::connect_and_start(endpoint, run_id, node_id.clone())
        .unwrap_or_else(|e| {
            let _ = std::fs::remove_dir_all(&work_dir);
            eprintln!("{operator_name}: failed to connect to control server: {e}");
            std::process::exit(1);
        });

    // 7. Call operator closure.
    let result = f(&payload, &work_dir);

    // 8. Clean up work dir (best-effort).
    let _ = std::fs::remove_dir_all(&work_dir);

    // 9. Stop heartbeat and report result.
    session.stop_heartbeat();
    match result {
        Ok(json) => {
            session.report_succeeded(json);
            std::process::exit(0);
        }
        Err((error_type, message, exit_code)) => {
            session.report_failed(error_type, message, exit_code);
            std::process::exit(0);
        }
    }
}
