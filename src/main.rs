use std::error::Error;

use rustyline::DefaultEditor;

mod engine;
mod expression;
mod ops;
mod schema;
mod table;
mod types;

use engine::{Engine, Output};

const HISTORY_PATH: &str = "history.txt";

fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let engine = Engine::new("camellia.db")?;
    let mut rl = DefaultEditor::new()?;
    if let Err(e) = rl.load_history(HISTORY_PATH) {
        println!("Failed to load history: {}", e);
    }

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

    rl.save_history(HISTORY_PATH)?;
    Ok(())
}
