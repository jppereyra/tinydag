//! A bounded pool of persistent Python worker processes for script syntax validation.
//!
//! Instead of spawning `python3 -m py_compile` for every script, callers
//! block until a pooled worker is available, send the script path over stdin,
//! and read the result from stdout. The pool is capacity-bounded so at most
//! [`capacity`] Python processes (and `2 × capacity` pipe file descriptors)
//! exist at any time, regardless of how many compilation tasks run concurrently.
//!
//! # Protocol
//!
//! The worker reads one absolute script path per line on stdin and responds
//! with either `"OK\n"` or `"ERROR: <message>\n"` on stdout, flushing before
//! reading the next line.
//!
//! # Concurrency
//!
//! The pool is safe to share across threads. `validate()` blocks the calling
//! thread until a worker is available; callers are expected to be in
//! `spawn_blocking` tasks where blocking is acceptable.

use std::io::{BufRead, BufReader, Write as _};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Condvar, Mutex, OnceLock};

use dashmap::DashSet;

/// Pool size used when `available_parallelism` can't be determined.
///
/// The global pool otherwise defaults to `2 × available_parallelism` capped at
/// 32, and can be set with `TINYDAG_PYTHON_POOL_SIZE`. Each worker uses two pipe
/// file descriptors in the tinydag process (stdin-write and stdout-read).
const DEFAULT_POOL_SIZE: usize = 16;

/// Python source run by each worker process.
///
/// Reads script paths from stdin (one per line) and checks each for syntax
/// errors using the `compile()` builtin. Unlike `py_compile.compile()`, the
/// builtin compiles entirely in memory — no `.pyc` file is written to disk,
/// which eliminates the marshal + I/O cost that makes `py_compile` CPU-heavy.
///
/// Writes `"OK\n"` or `"ERROR: <msg>\n"` to stdout, flushing after each
/// response. Newlines inside error messages are collapsed to spaces so the
/// single-line protocol is preserved.
const WORKER_SRC: &str = concat!(
    "import sys\n",
    "for raw in sys.stdin:\n",
    "    path = raw.rstrip('\\n')\n",
    "    try:\n",
    "        with open(path, 'rb') as f:\n",
    "            src = f.read()\n",
    "        compile(src, path, 'exec')\n",
    "        sys.stdout.write('OK\\n')\n",
    "    except SyntaxError as e:\n",
    "        sys.stdout.write('ERROR: ' + str(e).replace('\\n', ' ') + '\\n')\n",
    "    except Exception as e:\n",
    "        sys.stdout.write('ERROR: ' + str(e).replace('\\n', ' ') + '\\n')\n",
    "    sys.stdout.flush()\n",
);

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

struct Worker {
    /// Process handle kept alive so the OS does not reap the process.
    _child: Child,
    stdin: std::io::BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl Worker {
    fn spawn(python: &str) -> std::io::Result<Self> {
        let mut child = Command::new(python)
            .args(["-c", WORKER_SRC])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdin = std::io::BufWriter::new(child.stdin.take().unwrap());
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Ok(Worker {
            _child: child,
            stdin,
            stdout,
        })
    }

    /// Sends `script` to the worker and returns the validation result.
    fn compile(&mut self, script: &str) -> Result<(), String> {
        writeln!(self.stdin, "{script}").map_err(|e| format!("write to python worker: {e}"))?;
        self.stdin
            .flush()
            .map_err(|e| format!("flush python worker stdin: {e}"))?;

        let mut response = String::new();
        self.stdout
            .read_line(&mut response)
            .map_err(|e| format!("read from python worker: {e}"))?;

        if response.trim() == "OK" {
            Ok(())
        } else if let Some(msg) = response.strip_prefix("ERROR: ") {
            Err(msg.trim().to_string())
        } else if response.is_empty() {
            Err("python worker process exited unexpectedly".to_string())
        } else {
            Err(format!(
                "unexpected python worker response: {}",
                response.trim()
            ))
        }
    }
}

fn is_pipe_error(e: &str) -> bool {
    e.starts_with("write to python worker")
        || e.starts_with("read from python worker")
        || e.contains("unexpectedly")
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

struct PoolState {
    idle: Vec<Worker>,
    /// Number of workers currently checked out (not in `idle`).
    active: usize,
}

/// A capacity-bounded pool of reusable Python worker processes.
///
/// At most `capacity` workers exist at any point in time (idle + active).
/// Callers block in [`checkout`] if all workers are busy.
///
/// Successful results are memoized by absolute script path: a script that
/// passed once is never sent to a worker again within the same process
/// lifetime, regardless of how many pipelines reference it.
pub struct PythonPool {
    state: Mutex<PoolState>,
    /// Notified whenever a worker is checked back in or discarded.
    returned: Condvar,
    python: String,
    capacity: usize,
    /// Absolute paths of scripts that passed. Failures are not cached so a
    /// fixed script is re-checked.
    passed: DashSet<String>,
}

impl PythonPool {
    pub fn new(python: impl Into<String>, capacity: usize) -> Self {
        assert!(capacity > 0, "python pool capacity must be > 0");
        PythonPool {
            state: Mutex::new(PoolState {
                idle: Vec::new(),
                active: 0,
            }),
            returned: Condvar::new(),
            python: python.into(),
            capacity,
            passed: DashSet::new(),
        }
    }

    /// Checks out a worker, blocking until one is available.
    ///
    /// Returns an error if a new worker had to be spawned and spawning failed
    /// (e.g. the Python executable is missing).
    fn checkout(&self) -> Result<Worker, String> {
        let mut state = self.state.lock().unwrap();
        loop {
            // Prefer an already-warm idle worker.
            if let Some(w) = state.idle.pop() {
                state.active += 1;
                return Ok(w);
            }
            // Spawn a fresh worker if under capacity.
            if state.idle.len() + state.active < self.capacity {
                state.active += 1;
                drop(state); // release lock while spawning
                return Worker::spawn(&self.python).map_err(|e| {
                    self.discard();
                    format!("failed to start python worker ({}): {e}", self.python)
                });
            }
            // At capacity — wait for a worker to be returned.
            state = self.returned.wait(state).unwrap();
        }
    }

    /// Returns a healthy worker to the pool.
    fn checkin(&self, worker: Worker) {
        let mut state = self.state.lock().unwrap();
        state.active -= 1;
        state.idle.push(worker);
        self.returned.notify_one();
    }

    /// Releases the slot of a worker that is gone (pipe broken or never spawned).
    fn discard(&self) {
        let mut state = self.state.lock().unwrap();
        state.active -= 1;
        // Drop `state` before notifying so the woken thread can acquire the lock.
        drop(state);
        self.returned.notify_one();
    }

    /// Validates `script` using a pooled worker.
    ///
    /// Returns `None` on success, `Some(error_message)` on failure.
    /// Blocks until a worker is available. Successes are memoized: subsequent
    /// calls with the same path return immediately.
    pub fn validate(&self, script: &str) -> Option<String> {
        if self.passed.contains(script) {
            return None;
        }

        let mut worker = match self.checkout() {
            Ok(w) => w,
            Err(e) => return Some(e),
        };
        match worker.compile(script) {
            Ok(()) => {
                self.passed.insert(script.to_string());
                self.checkin(worker);
                None
            }
            Err(e) if is_pipe_error(&e) => {
                // Worker pipe broken — discard without caching (transient error).
                self.discard();
                Some(e)
            }
            Err(e) => {
                self.checkin(worker);
                Some(e)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Global entry point
// ---------------------------------------------------------------------------

/// The process-global bounded Python validation pool.
static POOL: OnceLock<PythonPool> = OnceLock::new();

/// Validates a Python script using the global bounded worker pool.
///
/// Pool capacity is read from `TINYDAG_PYTHON_POOL_SIZE` on first call,
/// defaulting to [`DEFAULT_POOL_SIZE`]. The Python executable is read from
/// `TINYDAG_PYTHON`, defaulting to `"python3"`.
///
/// Returns `None` on success, `Some(error_message)` on failure.
pub fn validate_script(script: &str) -> Option<String> {
    let pool = POOL.get_or_init(|| {
        let python = std::env::var("TINYDAG_PYTHON").unwrap_or_else(|_| "python3".to_string());
        let capacity = std::env::var("TINYDAG_PYTHON_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| (n.get() * 2).min(32))
                    .unwrap_or(DEFAULT_POOL_SIZE)
            });
        PythonPool::new(python, capacity)
    });
    pool.validate(script)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(capacity: usize) -> PythonPool {
        PythonPool::new("python3", capacity)
    }

    fn write_script(content: &str) -> tempfile::NamedTempFile {
        let f = tempfile::Builder::new()
            .suffix(".py")
            .tempfile()
            .expect("create temp file");
        std::fs::write(f.path(), content).expect("write temp file");
        f
    }

    #[test]
    fn valid_script_returns_none() {
        let f = write_script("x = 1 + 2\nprint(x)\n");
        assert!(pool(2).validate(&f.path().to_string_lossy()).is_none());
    }

    #[test]
    fn syntax_error_returns_some() {
        let f = write_script("def f(\n");
        assert!(pool(2).validate(&f.path().to_string_lossy()).is_some());
    }

    #[test]
    fn missing_file_returns_some() {
        assert!(
            pool(2)
                .validate("/tinydag_definitely_does_not_exist_xyz.py")
                .is_some()
        );
    }

    #[test]
    fn missing_python_returns_some_and_frees_slot() {
        let p = PythonPool::new("/tinydag_no_such_python_xyz", 1);
        let f = write_script("x = 1\n");
        let err = p.validate(&f.path().to_string_lossy()).unwrap();
        assert!(err.contains("failed to start python worker"), "{err}");
        assert_eq!(p.state.lock().unwrap().active, 0);
    }

    #[test]
    fn worker_is_reused_across_serial_calls() {
        let p = pool(2);
        let f1 = write_script("a = 1\n");
        let f2 = write_script("b = 2\n");
        assert!(p.validate(&f1.path().to_string_lossy()).is_none());
        assert!(p.validate(&f2.path().to_string_lossy()).is_none());
        // One worker was created and reused; it should be back in idle.
        let state = p.state.lock().unwrap();
        assert_eq!(state.idle.len(), 1);
        assert_eq!(state.active, 0);
    }

    #[test]
    fn pool_bounds_concurrency() {
        use std::sync::Arc;
        // Pool of 2 workers, 8 concurrent threads.
        let p = Arc::new(pool(2));
        let f = write_script("x = 1\n");
        let path = f.path().to_string_lossy().into_owned();

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let p = Arc::clone(&p);
                let path = path.clone();
                std::thread::spawn(move || {
                    assert!(p.validate(&path).is_none());
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // At most 2 workers should ever have existed.
        let state = p.state.lock().unwrap();
        assert!(state.idle.len() <= 2);
        assert_eq!(state.active, 0);
    }
}
