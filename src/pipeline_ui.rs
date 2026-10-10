//! Full-screen terminal UI for `tinydag add`, driven by [`AddEvent`]s.

use std::io::{self, Stdout};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    crossterm::{
        event::{self, Event, KeyCode, KeyModifiers},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};
use tinydag::scheduler::Scheduler;

use crate::batch::{self, AddArgs, AddEvent};

#[derive(Clone, Debug)]
pub enum PipelineStatus {
    Queued,
    Compiling,
    Registered,
    Running,
    Succeeded,
    Failed(String),
}

#[derive(Clone, Debug)]
pub struct PipelineEntry {
    pub path: String,
    pub status: PipelineStatus,
}

/// Runs the batch while rendering its progress, then waits for Ctrl+C.
///
/// Returns the batch exit code, or 130 if the user quit before it finished.
pub async fn run_add(add: AddArgs, scheduler: &Scheduler) -> i32 {
    let mut tui = match Tui::new() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("failed to initialize terminal UI: {e}");
            return 1;
        }
    };

    let mut pipelines: Vec<PipelineEntry> = add
        .paths
        .iter()
        .map(|p| PipelineEntry {
            path: p.clone(),
            status: PipelineStatus::Queued,
        })
        .collect();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let batch = batch::run_batch(add.paths, add.run_now, scheduler, tx);
    tokio::pin!(batch);

    let quit = ctrl_c_listener();
    let start = Instant::now();
    let mut metric_state = MetricState::default();
    let mut metrics = SystemMetrics::default();
    let mut sample = tokio::time::interval(Duration::from_millis(500));
    let mut tick = tokio::time::interval(Duration::from_millis(80));
    let mut summary = None;

    loop {
        tokio::select! {
            biased;
            _ = quit.notified() => break,
            s = &mut batch, if summary.is_none() => summary = Some(s),
            Some(event) = rx.recv() => apply_event(&mut pipelines, event),
            _ = sample.tick() => metrics = sample_metrics(&mut metric_state),
            _ = tick.tick() => {
                tui.render(&pipelines, &metrics, start).ok();
            }
        }
    }

    tui.shutdown();
    summary.map_or(130, |s| s.exit_code())
}

fn apply_event(pipelines: &mut [PipelineEntry], event: AddEvent) {
    let (idx, status) = match event {
        AddEvent::Compiling { idx } => (idx, PipelineStatus::Compiling),
        AddEvent::CompileFailed { idx, errors } => (idx, PipelineStatus::Failed(errors.join("; "))),
        AddEvent::Registered { idx, .. } => (idx, PipelineStatus::Registered),
        AddEvent::Running { idx } => (idx, PipelineStatus::Running),
        AddEvent::Succeeded { idx, .. } => (idx, PipelineStatus::Succeeded),
        AddEvent::RunFailed { idx, error } => (idx, PipelineStatus::Failed(error)),
    };
    pipelines[idx].status = status;
}

/// Spawns a blocking thread that polls crossterm key events and notifies when
/// Ctrl+C is pressed. Raw mode intercepts Ctrl+C before it becomes SIGINT, so
/// `tokio::signal::ctrl_c()` never fires — we have to read it as a key event.
fn ctrl_c_listener() -> Arc<tokio::sync::Notify> {
    let notify = Arc::new(tokio::sync::Notify::new());
    let notify2 = Arc::clone(&notify);
    tokio::task::spawn_blocking(move || {
        loop {
            if event::poll(Duration::from_millis(100)).unwrap_or(false)
                && let Ok(Event::Key(key)) = event::read()
                && key.code == KeyCode::Char('c')
                && key.modifiers.contains(KeyModifiers::CONTROL)
            {
                notify2.notify_one();
                return;
            }
        }
    });
    notify
}

pub struct Tui {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    restored: bool,
    compile_end: Option<Instant>,
}

impl Tui {
    pub fn new() -> io::Result<Self> {
        // Restore terminal on panic before printing the panic message.
        let original_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = disable_raw_mode();
            let _ = execute!(io::stdout(), LeaveAlternateScreen);
            original_hook(info);
        }));

        enable_raw_mode()?;
        let mut out = io::stdout();
        execute!(out, EnterAlternateScreen)?;
        let terminal = Terminal::new(CrosstermBackend::new(out))?;
        Ok(Self {
            terminal,
            restored: false,
            compile_end: None,
        })
    }

    pub fn render(
        &mut self,
        pipelines: &[PipelineEntry],
        metrics: &SystemMetrics,
        compile_start: Instant,
    ) -> io::Result<()> {
        self.terminal.draw(|frame| {
            let area = frame.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(0),
                    Constraint::Length(1),
                ])
                .split(area);

            let total = pipelines.len();
            let queued = pipelines
                .iter()
                .filter(|p| matches!(p.status, PipelineStatus::Queued))
                .count();
            let compiling = pipelines
                .iter()
                .filter(|p| matches!(p.status, PipelineStatus::Compiling))
                .count();
            let registered = pipelines
                .iter()
                .filter(|p| matches!(p.status, PipelineStatus::Registered))
                .count();
            let running = pipelines
                .iter()
                .filter(|p| matches!(p.status, PipelineStatus::Running))
                .count();
            let succeeded = pipelines
                .iter()
                .filter(|p| matches!(p.status, PipelineStatus::Succeeded))
                .count();
            let failed = pipelines
                .iter()
                .filter(|p| matches!(p.status, PipelineStatus::Failed(_)))
                .count();

            let mut pipeline_spans: Vec<Span> = vec![
                Span::styled(
                    "tinydag",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!("  {total} pipelines")),
            ];
            let add_stat = |spans: &mut Vec<Span>, n: usize, label: &str, color: Color| {
                if n > 0 {
                    spans.push(Span::raw("  │  "));
                    spans.push(Span::styled(
                        format!("{n} {label}"),
                        Style::default().fg(color),
                    ));
                }
            };
            add_stat(&mut pipeline_spans, queued, "queued", Color::DarkGray);
            add_stat(&mut pipeline_spans, compiling, "compiling", Color::Yellow);
            add_stat(&mut pipeline_spans, registered, "registered", Color::Green);
            add_stat(&mut pipeline_spans, running, "running", Color::Cyan);
            add_stat(&mut pipeline_spans, succeeded, "succeeded", Color::Green);
            add_stat(&mut pipeline_spans, failed, "failed", Color::Red);

            if queued + compiling == 0 && self.compile_end.is_none() {
                self.compile_end = Some(Instant::now());
            }
            let elapsed_dur = self
                .compile_end
                .unwrap_or_else(Instant::now)
                .duration_since(compile_start);
            let elapsed_secs = elapsed_dur.as_secs();
            let elapsed_str = if elapsed_secs >= 3600 {
                format!(
                    "Elapsed: {}h {:02}m",
                    elapsed_secs / 3600,
                    (elapsed_secs % 3600) / 60
                )
            } else if elapsed_secs >= 60 {
                format!("Elapsed: {}m {:02}s", elapsed_secs / 60, elapsed_secs % 60)
            } else {
                format!("Elapsed: {}s", elapsed_secs)
            };

            let fd_color = if metrics.fd_limit > 0 {
                let ratio = metrics.open_fds as f32 / metrics.fd_limit as f32;
                if ratio >= 0.90 {
                    Color::Red
                } else if ratio >= 0.75 {
                    Color::Yellow
                } else {
                    Color::DarkGray
                }
            } else {
                Color::DarkGray
            };
            let fd_str = if metrics.fd_limit > 0 {
                format!("fds: {} / {}", metrics.open_fds, metrics.fd_limit)
            } else {
                format!("fds: {}", metrics.open_fds)
            };

            let mut metrics_line = vec![
                Span::styled(fd_str, Style::default().fg(fd_color)),
                Span::raw("  │  "),
                Span::styled(
                    format!("cpu: {:.0}%", metrics.cpu_pct),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::raw("  │  "),
                Span::styled(
                    format!("mem: {:.0} MB", metrics.mem_mb),
                    Style::default().fg(Color::DarkGray),
                ),
            ];
            metrics_line.push(Span::raw("  │  "));
            metrics_line.push(Span::styled(
                elapsed_str,
                Style::default().fg(Color::DarkGray),
            ));

            frame.render_widget(
                Paragraph::new(vec![Line::from(pipeline_spans), Line::from(metrics_line)])
                    .block(Block::default().borders(Borders::BOTTOM)),
                chunks[0],
            );

            let mut items: Vec<ListItem> = Vec::new();
            let groups: &[(&str, &str, Color, usize)] = &[
                ("○", "queued     ", Color::DarkGray, queued),
                ("◌", "compiling  ", Color::Yellow, compiling),
                ("●", "registered ", Color::Green, registered),
                ("◎", "running    ", Color::Cyan, running),
                ("✓", "succeeded  ", Color::Green, succeeded),
                ("✗", "failed     ", Color::Red, failed),
            ];
            for (icon, label, color, count) in groups {
                if *count == 0 {
                    continue;
                }
                let s = Style::default().fg(*color);
                items.push(ListItem::new(Line::from(vec![
                    Span::styled(format!(" {icon} "), s.add_modifier(Modifier::BOLD)),
                    Span::styled(*label, s),
                    Span::styled(format!("{count}"), Style::default().fg(Color::White)),
                ])));
            }
            for entry in pipelines
                .iter()
                .filter(|p| matches!(p.status, PipelineStatus::Failed(_)))
            {
                let msg = match &entry.status {
                    PipelineStatus::Failed(m) => m.as_str(),
                    _ => "",
                };
                items.push(ListItem::new(Line::from(vec![
                    Span::raw("      "),
                    Span::styled(
                        "✗  ",
                        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(entry.path.clone(), Style::default().fg(Color::White)),
                    Span::styled(format!("    {msg}"), Style::default().fg(Color::DarkGray)),
                ])));
            }

            frame.render_widget(
                List::new(items).block(Block::default().borders(Borders::ALL).title(" pipelines ")),
                chunks[1],
            );

            frame.render_widget(
                Paragraph::new(" ctrl+c to exit").style(Style::default().fg(Color::DarkGray)),
                chunks[2],
            );
        })?;
        Ok(())
    }

    fn restore(&mut self) {
        if !self.restored {
            self.restored = true;
            let _ = disable_raw_mode();
            let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        }
    }

    pub fn shutdown(mut self) {
        self.restore();
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        self.restore();
    }
}

// ---------------------------------------------------------------------------
// System metrics sampling
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct SystemMetrics {
    pub open_fds: usize,
    pub fd_limit: usize,
    pub cpu_pct: f32,
    pub mem_mb: f32,
}

#[derive(Default)]
struct MetricState {
    prev_process_ticks: u64,
    prev_total_ticks: u64,
    prev_sample: Option<Instant>,
}

fn sample_metrics(state: &mut MetricState) -> SystemMetrics {
    SystemMetrics {
        open_fds: count_open_fds(),
        fd_limit: soft_fd_limit(),
        cpu_pct: sample_cpu(state),
        mem_mb: rss_mb(),
    }
}

fn count_open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

fn soft_fd_limit() -> usize {
    #[cfg(unix)]
    {
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `rl` is a valid, writable `rlimit` that outlives the call.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) } == 0 {
            return rl.rlim_cur as usize;
        }
    }
    0
}

fn sample_cpu(state: &mut MetricState) -> f32 {
    let (proc_ticks, total_ticks) = match read_cpu_ticks() {
        Some(v) => v,
        None => return 0.0,
    };
    let now = Instant::now();

    let cpu = if let Some(_prev_instant) = state.prev_sample {
        let d_proc = proc_ticks.saturating_sub(state.prev_process_ticks) as f64;
        let d_total = total_ticks.saturating_sub(state.prev_total_ticks) as f64;
        if d_total > 0.0 {
            let ncpu = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1) as f64;
            // d_proc / d_total is the fraction of one CPU; multiply by ncpu and 100 for %.
            ((d_proc / d_total) * ncpu * 100.0).min(100.0 * ncpu) as f32
        } else {
            0.0
        }
    } else {
        0.0
    };

    state.prev_process_ticks = proc_ticks;
    state.prev_total_ticks = total_ticks;
    state.prev_sample = Some(now);
    cpu
}

// Returns (process_ticks, system_total_ticks).
fn read_cpu_ticks() -> Option<(u64, u64)> {
    // Process ticks from /proc/self/stat: utime (field 14) + stime (field 15), 0-indexed.
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<&str> = stat.split_whitespace().collect();
    let utime: u64 = fields.get(13)?.parse().ok()?;
    let stime: u64 = fields.get(14)?.parse().ok()?;
    let proc_ticks = utime + stime;

    // System total from /proc/stat first line: cpu user nice system idle iowait irq softirq.
    let stat_line = std::fs::read_to_string("/proc/stat").ok()?;
    let cpu_line = stat_line.lines().next()?;
    let total_ticks: u64 = cpu_line
        .split_whitespace()
        .skip(1) // skip "cpu" label
        .map(|s| s.parse::<u64>().unwrap_or(0))
        .sum();

    Some((proc_ticks, total_ticks))
}

fn rss_mb() -> f32 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: f32 = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            return kb / 1024.0;
        }
    }
    0.0
}
