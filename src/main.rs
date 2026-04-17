use std::path::Path;
use std::sync::Arc;
use tinydag::compiler::CompileError;
use tinydag::executor::LocalExecutor;
use tinydag::runner::RunError;

#[tokio::main]
async fn main() {
    let providers = tinydag::telemetry::init();
    let args: Vec<String> = std::env::args().collect();

    let exit_code = match args.get(1).map(String::as_str) {
        Some("compile") => cmd_compile(&args),
        Some("run") => cmd_run(&args).await,
        _ => {
            eprintln!("usage: tinydag <command> [args]");
            eprintln!();
            eprintln!("commands:");
            eprintln!("  compile <pipeline.star> [--output <path>]   Compile a Starlark pipeline to a DAG");
            eprintln!("  run <pipeline.star>                          Compile and execute a Starlark pipeline");
            1
        }
    };

    providers.shutdown();

    if exit_code != 0 {
        std::process::exit(exit_code);
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
        let stem = p.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        let dir = p.parent().map(|d| d.to_string_lossy().into_owned()).unwrap_or_default();
        if dir.is_empty() {
            format!("{stem}.dag.json")
        } else {
            format!("{dir}/{stem}.dag.json")
        }
    });

    match tinydag::compiler::compile(Path::new(input)) {
        Ok(dag) => {
            let json = serde_json::to_string_pretty(&dag).expect("serialization cannot fail");
            if let Err(e) = std::fs::write(&output_path, json) {
                eprintln!("error: could not write '{output_path}': {e}");
                return 1;
            }
            println!("compiled {} nodes → {output_path}", dag.nodes().len());
            0
        }
        Err(CompileError::Eval(e)) => {
            eprintln!("error: {e}");
            1
        }
        Err(CompileError::Validation(errs)) => {
            for e in &errs {
                eprintln!("error: {e}");
            }
            1
        }
    }
}

async fn cmd_run(args: &[String]) -> i32 {
    let Some(path) = args.get(2) else {
        eprintln!("usage: tinydag run <pipeline.star>");
        return 1;
    };

    let dag = match tinydag::compiler::compile(Path::new(path)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    let dag_id = dag.dag_id().to_string();
    let executor = Arc::new(LocalExecutor::new().await);
    let scheduler = tinydag::scheduler::Scheduler::new(executor);
    scheduler.register(dag);
    match scheduler.trigger(&dag_id).unwrap().await.unwrap() {
        Ok(outcome) => {
            println!(
                "run {} succeeded ({} tasks)",
                outcome.run_id,
                outcome.succeeded.len()
            );
            0
        }
        Err(RunError::TaskFailed { node_id, source }) => {
            eprintln!("task '{node_id}' failed: {source}");
            3
        }
        Err(RunError::DuplicateOutput { node_id, output_name }) => {
            eprintln!("error: predecessors of '{node_id}' produce duplicate output name '{output_name}'");
            3
        }
    }
}
