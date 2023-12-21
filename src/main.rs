use std::error::Error;

use rustyline::DefaultEditor;

mod engine;
mod types;

use engine::Engine;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

const HISTORY_PATH: &str = "history.txt";

fn main() -> Result<(), Box<dyn Error>> {
    let dialect = GenericDialect {};
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

        match Parser::parse_sql(&dialect, line) {
            Ok(program) => match engine.run(program) {
                Ok(None) => {}
                Ok(Some(rowset)) => {
                    println!("{}", rowset);
                }
                Err(e) => {
                    println!("Query failed: {}", e);
                }
            },
            Err(e) => {
                println!("Failed to parse query: {}", e)
            }
        }

        rl.add_history_entry(line)?;
    }

    rl.save_history(HISTORY_PATH)?;
    Ok(())
}
