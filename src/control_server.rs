//! gRPC control server for the operator protocol.
//!
//! Runs a tonic server on the caller's tokio runtime. The executor registers
//! `(run_id, task_id)` pairs before spawning operators and receives
//! `ControlEvent`s via bounded `tokio::sync::mpsc` channels (capacity 64).
//! Back-pressure is applied at the gRPC layer as the operator's `Ack` is not
//! returned until the event is accepted into the channel.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

pub mod proto {
    tonic::include_proto!("operator_control");
}

use proto::operator_control_server::{OperatorControl, OperatorControlServer};
use proto::{Ack, FailedEvent, HeartbeatEvent, StartedEvent, SucceededEvent};
use tonic::{Request, Response, Status, transport::Server};

// ---------------------------------------------------------------------------
// Public event types
// ---------------------------------------------------------------------------

/// An event delivered from an operator binary.
#[derive(Debug)]
pub enum ControlEvent {
    Started,
    Heartbeat,
    Succeeded {
        outputs_json: String,
    },
    Failed {
        error_type: proto::FailedErrorType,
        message: String,
        exit_code: i32,
    },
}

impl ControlEvent {
    fn kind(&self) -> &'static str {
        match self {
            ControlEvent::Started => "started",
            ControlEvent::Heartbeat => "heartbeat",
            ControlEvent::Succeeded { .. } => "succeeded",
            ControlEvent::Failed { .. } => "failed",
        }
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

const CHANNEL_CAPACITY: usize = 64;

type Senders = Arc<DashMap<(String, String), tokio::sync::mpsc::Sender<ControlEvent>>>;

/// RAII guard that removes a registered task sender when dropped.
///
/// Prevents sender map leaks on cancellation or early return.
pub struct TaskGuard {
    senders: Senders,
    run_id: String,
    task_id: String,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.senders
            .remove(&(self.run_id.clone(), self.task_id.clone()));
    }
}

// ---------------------------------------------------------------------------
// Internal gRPC service
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct GrpcService {
    senders: Senders,
    unknown_task_events: Arc<AtomicU64>,
}

impl GrpcService {
    async fn route(&self, run_id: &str, task_id: &str, event: ControlEvent) {
        // Clone the sender to release the DashMap shard lock before awaiting.
        let tx = self
            .senders
            .get(&(run_id.to_string(), task_id.to_string()))
            .map(|r| r.clone());
        match tx {
            Some(tx) => {
                let _ = tx.send(event).await;
            }
            // Usually a late report from an operator whose dispatch already
            // ended (timed out or cancelled).
            None => {
                self.unknown_task_events.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    run.id = %run_id,
                    task.id = %task_id,
                    event = event.kind(),
                    "dropping control event for a task that is not running"
                );
            }
        }
    }
}

#[tonic::async_trait]
impl OperatorControl for GrpcService {
    async fn report_started(&self, req: Request<StartedEvent>) -> Result<Response<Ack>, Status> {
        let e = req.into_inner();
        self.route(&e.run_id, &e.task_id, ControlEvent::Started)
            .await;
        Ok(Response::new(Ack {}))
    }

    async fn report_heartbeat(
        &self,
        req: Request<HeartbeatEvent>,
    ) -> Result<Response<Ack>, Status> {
        let e = req.into_inner();
        self.route(&e.run_id, &e.task_id, ControlEvent::Heartbeat)
            .await;
        Ok(Response::new(Ack {}))
    }

    async fn report_succeeded(
        &self,
        req: Request<SucceededEvent>,
    ) -> Result<Response<Ack>, Status> {
        let e = req.into_inner();
        self.route(
            &e.run_id,
            &e.task_id,
            ControlEvent::Succeeded {
                outputs_json: e.outputs_json,
            },
        )
        .await;
        Ok(Response::new(Ack {}))
    }

    async fn report_failed(&self, req: Request<FailedEvent>) -> Result<Response<Ack>, Status> {
        let e = req.into_inner();
        let error_type = proto::FailedErrorType::try_from(e.error_type)
            .unwrap_or(proto::FailedErrorType::Unspecified);
        self.route(
            &e.run_id,
            &e.task_id,
            ControlEvent::Failed {
                error_type,
                message: e.message,
                exit_code: e.exit_code,
            },
        )
        .await;
        Ok(Response::new(Ack {}))
    }
}

// ---------------------------------------------------------------------------
// Public ControlServer
// ---------------------------------------------------------------------------

pub struct ControlServer {
    endpoint: String,
    senders: Senders,
    unknown_task_events: Arc<AtomicU64>,
}

impl ControlServer {
    pub async fn start() -> std::io::Result<Self> {
        let senders: Senders = Arc::new(DashMap::new());
        let unknown_task_events = Arc::new(AtomicU64::new(0));
        let svc = GrpcService {
            senders: Arc::clone(&senders),
            unknown_task_events: Arc::clone(&unknown_task_events),
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        tokio::task::spawn(async move {
            Server::builder()
                .add_service(OperatorControlServer::new(svc))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("control server: serve failed");
        });

        Ok(ControlServer {
            endpoint: format!("http://127.0.0.1:{}", addr.port()),
            senders,
            unknown_task_events,
        })
    }

    /// Control events received for tasks that weren't registered (and were dropped).
    pub fn unknown_task_events(&self) -> u64 {
        self.unknown_task_events.load(Ordering::Relaxed)
    }

    /// The HTTP/2 endpoint that operator binaries should connect to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Register a task and return a receiver.
    ///
    /// Must be called before the operator binary is spawned so that events
    /// sent immediately after startup are not lost.
    pub fn register(
        &self,
        run_id: String,
        task_id: String,
    ) -> (tokio::sync::mpsc::Receiver<ControlEvent>, TaskGuard) {
        let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
        self.senders.insert((run_id.clone(), task_id.clone()), tx);
        let guard = TaskGuard {
            senders: Arc::clone(&self.senders),
            run_id,
            task_id,
        };
        (rx, guard)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::proto::operator_control_client::OperatorControlClient;
    use super::*;

    fn started(run_id: &str, task_id: &str) -> StartedEvent {
        StartedEvent {
            run_id: run_id.to_string(),
            task_id: task_id.to_string(),
        }
    }

    #[tokio::test]
    async fn events_reach_the_registered_task() {
        let server = ControlServer::start().await.unwrap();
        let (mut rx, _guard) = server.register("run".into(), "task".into());
        let mut client = OperatorControlClient::connect(server.endpoint().to_string())
            .await
            .unwrap();

        client.report_started(started("run", "task")).await.unwrap();
        assert!(matches!(rx.recv().await, Some(ControlEvent::Started)));
        assert_eq!(server.unknown_task_events(), 0);
    }

    #[tokio::test]
    async fn events_for_unknown_tasks_are_counted() {
        let server = ControlServer::start().await.unwrap();
        let mut client = OperatorControlClient::connect(server.endpoint().to_string())
            .await
            .unwrap();

        client
            .report_started(started("run", "nobody"))
            .await
            .unwrap();
        assert_eq!(server.unknown_task_events(), 1);
    }
}
