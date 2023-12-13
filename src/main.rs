use std::error::Error;
use std::str::FromStr;

use rustyline::DefaultEditor;

mod query;

use query::Query;

fn main() -> Result<(), Box<dyn Error>> {
    let mut rl = DefaultEditor::new()?;
    while let Ok(line) = rl.readline("# ") {
        match Query::from_str(&line) {
            Ok(query) => println!("{:#?}", query),
            Err(e) => eprintln!("Failed to parse query: {}", e),
        }

        rl.add_history_entry(line)?;
    }

    Ok(())
}
