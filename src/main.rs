mod batch;
mod pipeline_ui;

use std::io::IsTerminal as _;
use std::path::Path;
use std::sync::Arc;

use tinydag::compiler::CompileError;
use tinydag::executor::LocalExecutor;
use tinydag::scheduler::Scheduler;

/// Discover `tinydag-op-*` binaries that live alongside the current executable.
///
/// Returns `(OPERATOR_UPPERCASE, binary_path)` pairs.  Any operator whose
/// `TINYDAG_OP_<NAME>` environment variable is already set is skipped so that
/// explicit env-var overrides always take precedence over auto-discovery.
fn discover_op_binaries() -> Vec<(String, String)> {
    let Ok(exe) = std::env::current_exe() else {
        return vec![];
    };
    let Some(dir) = exe.parent() else {
        return vec![];
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return vec![];
    };

    let mut found = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let Some(op) = name_str.strip_prefix("tinydag-op-") else {
            continue;
        };
        if op.is_empty() {
            continue;
        }

        let op_upper = op.to_ascii_uppercase();

        // Env var beats auto-discovery.
        if std::env::var(format!("TINYDAG_OP_{op_upper}")).is_ok() {
            continue;
        }

        let Ok(meta) = entry.metadata() else { continue };
        if !meta.is_file() {
            continue;
        }

        // On Unix, skip files that aren't executable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if meta.permissions().mode() & 0o111 == 0 {
                continue;
            }
        }

        found.push((op_upper, entry.path().to_string_lossy().into_owned()));
    }
    found
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    // The TUI owns the terminal, so console logging would corrupt it.
    let suppress_console =
        args.get(1).is_some_and(|s| s == "add") && std::io::stdout().is_terminal();
    let providers = tinydag::telemetry::init(suppress_console);

    let exit_code = match args.get(1).map(String::as_str) {
        Some("compile") => cmd_compile(&args),
        Some("add") => cmd_add(&args).await,
        _ => {
            eprintln!("usage: tinydag <command> [args]");
            eprintln!();
            eprintln!("commands:");
            eprintln!(
                "  compile <pipeline.star> [--output <path>]   Compile a Starlark pipeline to a DAG"
            );
            eprintln!(
                "  add <pipeline.star> [<pipeline.star> ...] [--file <list>] [--run-now]\n      Register pipelines with the scheduler"
            );
            1
        }
    };

    providers.shutdown();

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn print_compile_errors(path: &str, e: CompileError) {
    match e {
        CompileError::Eval(e) => eprintln!("error in '{path}': {e}"),
        CompileError::Validation(errs) => {
            for e in errs {
                eprintln!("error in '{path}': {e}");
            }
        }
    }
}

fn cmd_compile(args: &[String]) -> i32 {
    let Some(input) = args.get(2) else {
        eprintln!("usage: tinydag compile <pipeline.star> [--output <path>]");
        return 1;
    };

    // Parse --output <path> if present.
    let mut output: Option<String> = None;
    let mut i = 3;
    while i + 1 < args.len() {
        if args[i] == "--output" {
            output = Some(args[i + 1].clone());
            i += 2;
        } else {
            i += 1;
        }
    }

    // Default output: <stem>.dag.json beside the input file.
    let output_path = output.unwrap_or_else(|| {
        let p = Path::new(input);
        let stem = p
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let dir = p
            .parent()
            .map(|d| d.to_string_lossy().into_owned())
            .unwrap_or_default();
        if dir.is_empty() {
            format!("{stem}.dag.json")
        } else {
            format!("{dir}/{stem}.dag.json")
        }
    });

    let path = Path::new(input);
    let source = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot read '{input}': {e}");
            return 1;
        }
    };
    match tinydag::compiler::compile(input, &source, path.parent()) {
        Ok(dag) => {
            let json = serde_json::to_string_pretty(&dag).expect("serialization cannot fail");
            if let Err(e) = std::fs::write(&output_path, json) {
                eprintln!("error: could not write '{output_path}': {e}");
                return 1;
            }
            println!("compiled {} nodes → {output_path}", dag.nodes().len());
            0
        }
        Err(e) => {
            print_compile_errors(input, e);
            1
        }
    }
}

async fn cmd_add(args: &[String]) -> i32 {
    let add = match batch::parse_add_args(args) {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            return 1;
        }
    };

    let mut executor = LocalExecutor::new().await;
    for (op, path) in discover_op_binaries() {
        executor = executor.with_op_binary(&op, path);
    }
    let scheduler = Scheduler::new(Arc::new(executor));

    if std::io::stdout().is_terminal() {
        return pipeline_ui::run_add(add, &scheduler).await;
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let paths = add.paths.clone();
    let printer = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            batch::print_event(&paths, &event);
        }
    });
    let summary = batch::run_batch(add.paths, add.run_now, &scheduler, tx).await;
    printer.await.expect("printer task panicked");
    summary.exit_code()
}
