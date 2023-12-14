use std::error::Error;
use std::str::FromStr;

use rustyline::DefaultEditor;

mod engine;
mod query;

use engine::Engine;
use query::Query;

fn main() -> Result<(), Box<dyn Error>> {
    let engine = Engine::new("camellia.db")?;
    let mut rl = DefaultEditor::new()?;
    while let Ok(line) = rl.readline("> ") {
        match Query::from_str(&line) {
            Ok(query) => match engine.run(query) {
                Ok(None) => {}
                Ok(Some(rowset)) => {
                    println!("{}", rowset);
                }
                Err(e) => {
                    eprintln!("Query failed: {}", e);
                }
            },
            Err(e) => eprintln!("Failed to parse query: {}", e),
        }

        rl.add_history_entry(line)?;
    }

    Ok(())
}
