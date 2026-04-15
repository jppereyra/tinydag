use std::sync::Arc;
use tinydag::executor::LocalExecutor;
use tinydag::ir::DagDef;
use tinydag::runner::RunError;
use tinydag::validation::validate;

#[tokio::main]
async fn main() {
    let providers = tinydag::telemetry::init();
    let args: Vec<String> = std::env::args().collect();

    let exit_code = match args.get(1).map(String::as_str) {
        Some("run") => cmd_run(&args).await,
        _ => {
            eprintln!("usage: tinydag <command> [args]");
            eprintln!();
            eprintln!("commands:");
            eprintln!("  run <dag.json>   Execute a DAG from a JSON IR file");
            1
        }
    };

    providers.shutdown();

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

async fn cmd_run(args: &[String]) -> i32 {
    let Some(path) = args.get(2) else {
        eprintln!("usage: tinydag run <dag.json>");
        return 1;
    };

    let json = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: could not read '{path}': {e}");
            return 1;
        }
    };

    let dag: DagDef = match serde_json::from_str(&json) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: could not parse '{path}' as a DAG IR: {e}");
            return 1;
        }
    };

    if let Err(errors) = validate(&dag) {
        eprintln!("validation failed:");
        for e in &errors {
            eprintln!("  {e}");
        }
        return 2;
    }

    let dag_id = dag.dag_id.clone();
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
