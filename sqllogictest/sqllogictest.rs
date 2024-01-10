use std::{
    error::Error,
    fmt,
    path::{Path, PathBuf},
};

use camellia::{Column, Engine, Output, RowSet, Type};
use sqllogictest::{
    harness::{self, glob, Arguments, Failed, Trial},
    DBOutput, DefaultColumnType, MakeConnection, Runner,
};
struct BoxError(Box<dyn Error + Send + Sync + 'static>);

impl fmt::Display for BoxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for BoxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for BoxError {}

struct Database {
    engine: Engine,
}

impl Database {
    fn new() -> Self {
        const PATH: &str = "camellia.test.db";

        if std::path::Path::new(PATH).exists() {
            std::fs::remove_dir_all(PATH).unwrap();
        }

        Database {
            engine: Engine::new(PATH).unwrap(),
        }
    }
}

impl sqllogictest::DB for Database {
    type Error = BoxError;
    type ColumnType = DefaultColumnType;

    fn engine_name(&self) -> &str {
        "camellia"
    }

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        match self.engine.run_sql(sql) {
            Ok(Output::Rows(rows)) => Ok(convert(rows)),
            Ok(Output::Affected(n)) => Ok(DBOutput::StatementComplete(n as u64)),
            Err(e) => Err(BoxError(e)),
        }
    }
}

fn type_of(column: &Column) -> DefaultColumnType {
    match column.type_ {
        Type::Null => DefaultColumnType::Any,
        Type::Integer => DefaultColumnType::Integer,
        Type::Text => DefaultColumnType::Text,
        Type::Bool => DefaultColumnType::Any,
    }
}

fn convert(rowset: RowSet) -> DBOutput<DefaultColumnType> {
    let types = rowset.schema.columns().map(type_of).collect();
    let rows = rowset
        .rows
        .iter()
        .map(|row| row.values().map(|val| val.to_string()).collect())
        .collect();

    DBOutput::Rows { types, rows }
}

struct Sqlite(rusqlite::Connection);

impl Sqlite {
    fn new() -> Self {
        let c = rusqlite::Connection::open_in_memory().unwrap();
        Sqlite(c)
    }
}

fn value_to_string(v: rusqlite::types::ValueRef) -> String {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Integer(i) => i.to_string(),
        ValueRef::Real(r) => r.to_string(),
        ValueRef::Text(s) => std::str::from_utf8(s).unwrap().to_string(),
        ValueRef::Blob(_) => todo!(),
    }
}

impl sqllogictest::DB for Sqlite {
    type Error = rusqlite::Error;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, Self::Error> {
        let mut output = vec![];

        let is_query_sql = {
            let lower_sql = sql.trim_start().to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
        };

        if is_query_sql {
            let mut stmt = self.0.prepare(sql)?;
            let column_count = stmt.column_count();
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let mut row_output = vec![];
                for i in 0..column_count {
                    let row = row.get_ref(i)?;
                    row_output.push(value_to_string(row));
                }
                output.push(row_output);
            }
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Any; column_count],
                rows: output,
            })
        } else {
            let cnt = self.0.execute(sql, [])?;
            Ok(DBOutput::StatementComplete(cnt as u64))
        }
    }

    fn engine_name(&self) -> &str {
        "sqlite"
    }
}

fn test_files() -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    // Links:
    // - https://github.com/risinglightdb/sqllogictest-sqlite
    // - https://github.com/MaterializeInc/materialize/tree/main/test/sqllogictest
    // - https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/logictest/testdata/logic_test
    // - https://github.com/duckdb/duckdb/tree/main/test/sql
    let pattern = "sqllogictest/*.test";
    let paths = glob(pattern)
        .expect("failed to find test files")
        .collect::<Result<Vec<_>, _>>()?;
    // TODO: make it pass
    // paths.push(PathBuf::from("sqllogictest/sqlite-tests/test/select1.test"));
    Ok(paths)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut tests = vec![];
    for path in test_files()? {
        tests.push(Trial::test(path.to_str().unwrap().to_string(), move || {
            test(
                &path,
                || async { Ok(Database::new()) },
                || async { Ok(Sqlite::new()) },
            )
        }));
    }

    if tests.is_empty() {
        return Err("No tests found".into());
    }

    let mut args = Arguments::from_args();
    // TODO: support multithreaded testing
    args.test_threads = Some(1);
    harness::run(&args, tests).exit();
}

fn test(
    filename: impl AsRef<Path>,
    make_conn: impl MakeConnection,
    make_sqlite_conn: impl MakeConnection,
) -> Result<(), Failed> {
    let filename = filename.as_ref();
    let mut sqlite_tester = Runner::new(make_sqlite_conn);
    futures::executor::block_on(sqlite_tester.update_test_file(
        filename,
        " ",
        sqllogictest::default_validator,
        sqllogictest::default_column_validator,
    ))?;

    let mut tester = Runner::new(make_conn);
    tester.run_file(filename)?;
    Ok(())
}
