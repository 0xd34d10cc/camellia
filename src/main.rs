use std::error::Error;

use minitrace::collector::SpanContext;
use rustyline::DefaultEditor;

mod engine;
mod expression;
mod ops;
mod schema;
mod table;
mod trace;
mod types;

use engine::{Engine, Output};

const HISTORY_PATH: &str = "history.txt";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    if let Err(e) = dotenvy::dotenv() {
        eprintln!("Faield to read .env file: {}", e);
    }

    if let Ok(endpoint) = std::env::var("CAMELLIA_TRACE") {
        if let Err(e) = trace::init(endpoint) {
            eprintln!("Failed to initialize tracing: {}", e)
        }
    }

    let mut rl = DefaultEditor::new()?;
    if let Err(e) = rl.load_history(HISTORY_PATH) {
        println!("Failed to load history: {}", e);
    }

    let engine = Engine::new("camellia.db")?;
    while let Ok(line) = rl.readline("> ") {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line == ":log on" {
            engine.set_log(true);
            continue;
        } else if line == ":log off" {
            engine.set_log(false);
            continue;
        }

        let span = minitrace::Span::root("query", SpanContext::random())
            .with_property(|| ("query", line.to_owned()));
        let _guard = span.set_local_parent();

        match engine.run_sql(line) {
            Ok(Output::Affected(n)) => {
                if n != 0 {
                    println!("{} row(s) affected", n);
                }
            }
            Ok(Output::Rows(rowset)) => {
                println!("{}", rowset);
            }
            Err(e) => {
                println!("Query failed: {}", e);
            }
        }

        rl.add_history_entry(line)?;
    }

    trace::shutdown();
    rl.save_history(HISTORY_PATH)?;
    Ok(())
}
