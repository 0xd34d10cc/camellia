use std::{error::Error, fmt, path::Path};

use camellia::{Engine, Output, RowSet};
use sqllogictest::{
    harness::{self, glob, Arguments, Failed, Trial},
    DBOutput, DefaultColumnType, MakeConnection, Runner,
};
use sqlparser::ast::{self, ColumnDef};

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

fn type_of(column: &ColumnDef) -> DefaultColumnType {
    match &column.data_type {
        ast::DataType::Int(None) => DefaultColumnType::Integer,
        ast::DataType::Text => DefaultColumnType::Text,
        _ => DefaultColumnType::Any,
    }
}

fn convert(rowset: RowSet) -> DBOutput<DefaultColumnType> {
    let types = rowset.schema.columns.iter().map(type_of).collect();
    let rows = rowset
        .rows
        .iter()
        .map(|row| row.values().map(|val| val.to_string()).collect())
        .collect();

    DBOutput::Rows { types, rows }
}

fn main() {
    // TODO: integrate https://github.com/risinglightdb/sqllogictest-sqlite
    // also, see https://github.com/duckdb/duckdb/tree/main/test/sql
    // let pattern = "sqllogictest/sqllogictest-sqlite/test/**/*.test";
    let pattern = "sqllogictest/*.test";
    let paths = glob(pattern).expect("failed to find test files");
    let mut tests = vec![];

    for entry in paths {
        let path = entry.expect("failed to read glob entry");
        tests.push(Trial::test(path.to_str().unwrap().to_string(), move || {
            test(&path, || async { Ok(Database::new()) })
        }));
    }

    if tests.is_empty() {
        panic!("no test found for sqllogictest under: {}", pattern);
    }

    harness::run(&Arguments::from_args(), tests).exit();
}

fn test(filename: impl AsRef<Path>, make_conn: impl MakeConnection) -> Result<(), Failed> {
    let mut tester = Runner::new(make_conn);
    tester.run_file(filename)?;
    Ok(())
}
