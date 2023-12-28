use std::path::Path;

use rocksdb::{IteratorMode, Options, Transaction};
use sqlparser::ast::{
    self, ColumnDef, Expr, GroupByExpr, HiveDistributionStyle, HiveFormat, ObjectName, ObjectType,
    Query, Select, SetExpr, Statement, TableFactor, TableWithJoins, Values,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::ops::{self, FullScan, Operation, Projection};
use crate::types::{Database, Result, Row, RowSet, Schema, Type, Value};

pub enum Output {
    Rows(RowSet),
    Affected(usize),
}

pub struct Engine {
    db: Database,
}

impl Engine {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let txn_db_opts = rocksdb::TransactionDBOptions::default();
        let column_families = if path.exists() {
            Database::list_cf(&opts, path)?
        } else {
            Vec::new()
        };
        let db = Database::open_cf(&opts, &txn_db_opts, path, column_families)?;
        Ok(Engine { db })
    }

    pub fn run_sql(&self, program: &str) -> Result<Output> {
        let dialect = GenericDialect {};
        let program = Parser::parse_sql(&dialect, program)?;
        self.run(program)
    }

    pub fn run(&self, program: Vec<Statement>) -> Result<Output> {
        if program.len() != 1 {
            return Err("Cannot run more than one statement at time".into());
        }

        let statement = program.into_iter().next().unwrap();
        self.execute(statement)
    }

    fn execute(&self, statement: Statement) -> Result<Output> {
        match statement {
            Statement::CreateTable {
                name,
                columns,
                auto_increment_offset: None,
                or_replace: false,
                temporary: false,
                external: false,
                global: None,
                if_not_exists: false,
                transient: false,
                constraints,
                hive_distribution: HiveDistributionStyle::NONE,
                hive_formats:
                    Some(HiveFormat {
                        row_format: None,
                        storage: None,
                        location: None,
                    }),
                table_properties,
                with_options,
                file_format: None,
                location: None,
                query: None,
                without_rowid: false,
                like: None,
                clone: None,
                engine: None,
                comment: None,
                default_charset: None,
                collation: None,
                on_commit: None,
                on_cluster: None,
                order_by: None,
                strict: false,
            } if constraints.is_empty()
                && table_properties.is_empty()
                && with_options.is_empty() =>
            {
                self.create(name, columns)?;
                Ok(Output::Affected(0))
            }
            Statement::Drop {
                object_type: ObjectType::Table,
                if_exists: false,
                names,
                cascade: false,
                restrict: false,
                purge: false,
                temporary: false,
            } if names.len() == 1 => {
                self.drop(names.into_iter().next().unwrap())?;
                Ok(Output::Affected(0))
            }
            Statement::Query(query) => {
                let rows = self.query(*query)?;
                Ok(rows)
            }
            Statement::Insert {
                or: None,
                ignore: false,
                into: true,
                table_name,
                columns,
                overwrite: false,
                source: Some(source),
                partitioned: None,
                after_columns,
                table: false,
                on: None,
                returning: None,
            } if columns.is_empty() && after_columns.is_empty() => {
                self.insert(table_name, *source)?;
                Ok(Output::Affected(1))
            }
            _ => Err("Not supported".into()),
        }
    }

    fn query(&self, query: Query) -> Result<Output> {
        let query = match query {
            Query {
                with: None,
                body,
                order_by,
                limit: None,
                limit_by,
                offset: None,
                fetch: None,
                locks,
                for_clause: None,
            } if order_by.is_empty() && limit_by.is_empty() && locks.is_empty() => *body,
            _ => return Err("Not implemented".into()),
        };

        match query {
            SetExpr::Select(select) => self.select(*select),
            _ => Err("Not supported".into()),
        }
    }

    fn create(&self, name: ObjectName, columns: Vec<ColumnDef>) -> Result<()> {
        let table = name.to_string();
        for column in &columns {
            type_of(column)?;
        }

        let primary_keys = columns
            .iter()
            .filter(|column| is_primary_key(column))
            .count();
        if primary_keys != 1 {
            return Err("Exactly one field of table must be marked as primary key".into());
        }

        let opts = Options::default();
        self.db.create_cf(&table, &opts)?;

        let transaction = self.db.transaction();
        if transaction.get(&table)?.is_some() {
            return Err("Table with such name already exist, but shouldn't".into());
        }

        let schema = Schema { columns };
        let schema = bincode::serialize(&schema)?;
        transaction.put(&table, schema)?;
        transaction.commit()?;
        Ok(())
    }

    fn drop(&self, name: ObjectName) -> Result<()> {
        let table = name.to_string();
        let transaction = self.db.transaction();
        transaction.delete(&table)?;
        transaction.commit()?;
        self.db.drop_cf(&table)?;
        Ok(())
    }

    fn insert(&self, name: ObjectName, source: Query) -> Result<()> {
        let expr = match source {
            Query {
                with: None,
                body,
                order_by,
                limit: None,
                limit_by,
                offset: None,
                fetch: None,
                locks,
                for_clause: None,
            } if order_by.is_empty() && limit_by.is_empty() && locks.is_empty() => *body,
            _ => return Err("Unsupported insert statement kind".into()),
        };

        let row = match expr {
            SetExpr::Values(values) => create_row(values)?,
            _ => return Err("Unsupported insert expression kind".into()),
        };

        let table = name.to_string();
        let cf = self.db.cf_handle(&table).ok_or("No such table")?;

        let transaction = self.db.transaction();
        let schema = self.read_schema(&table, &transaction)?;
        let primary_key = schema.check(&row)?;
        let mut key = Vec::new();
        match row.get(primary_key) {
            Value::Int(val) => key.extend(val.to_be_bytes()),
            Value::Bool(val) => key.push(*val as u8),
            Value::String(val) => key.extend(val.as_bytes()),
        };

        if transaction.get_for_update_cf(&cf, &key, true)?.is_some() {
            return Err("Entry with such primary key already exist".into());
        }

        let mut value = Vec::new();
        row.serialize(&mut value)?;
        transaction.put_cf(&cf, &key, value)?;
        transaction.commit()?;
        Ok(())
    }

    fn select(&self, query: Select) -> Result<Output> {
        let (table, projection) = match query {
            Select {
                distinct: None,
                top: None,
                projection,
                into: None,
                from,
                lateral_views,
                selection: None,
                group_by: GroupByExpr::Expressions(group_by_exprs),
                cluster_by,
                distribute_by,
                sort_by,
                having: None,
                named_window,
                qualify: None,
            } if from.len() == 1
                && lateral_views.is_empty()
                && group_by_exprs.is_empty()
                && cluster_by.is_empty()
                && distribute_by.is_empty()
                && sort_by.is_empty()
                && named_window.is_empty() =>
            {
                match from.into_iter().next().unwrap() {
                    TableWithJoins {
                        relation:
                            TableFactor::Table {
                                name,
                                alias: None,
                                args: None,
                                with_hints,
                                version: None,
                                partitions,
                            },
                        joins,
                    } if joins.is_empty() && with_hints.is_empty() && partitions.is_empty() => {
                        (name.to_string(), projection)
                    }
                    _ => return Err("Unsupported select source".into()),
                }
            }
            _ => return Err("Unsupported select kind".into()),
        };

        let cf = self.db.cf_handle(&table).ok_or("No such table")?;
        let transaction = self.db.transaction();
        let schema = self.read_schema(&table, &transaction)?;

        let iter = transaction.iterator_cf(&cf, IteratorMode::Start);
        let scan = FullScan::new(schema, iter)?;
        let mut projection = Projection::new(&projection, Box::new(scan))?;

        let schema = projection.schema().clone();
        let mut rows = Vec::new();
        loop {
            match projection.poll() {
                Ok(ops::Output::Finished) => break Ok(Output::Rows(RowSet { rows, schema })),
                Ok(ops::Output::Batch(mut batch)) => {
                    rows.append(&mut batch);
                }
                Err(e) => break Err(e),
            }
        }
    }

    fn read_schema(&self, table: &str, transaction: &Transaction<'_, Database>) -> Result<Schema> {
        let bytes = transaction
            .get(table)?
            .ok_or("Schema for this table not found")?;
        let schema = bincode::deserialize(&bytes)?;
        Ok(schema)
    }
}

fn is_primary_key(column: &ColumnDef) -> bool {
    use sqlparser::ast::ColumnOption;

    column
        .options
        .iter()
        .any(|option| matches!(option.option, ColumnOption::Unique { is_primary: true }))
}

fn map_value(value: ast::Value) -> Result<Value> {
    let value = match value {
        ast::Value::Boolean(val) => Value::Bool(val),
        ast::Value::Number(number, false) => Value::Int(number.parse::<i64>()?),
        ast::Value::SingleQuotedString(string) => Value::String(string),
        _ => return Err("Unsupported value type".into()),
    };

    Ok(value)
}

fn create_row(values: Values) -> Result<Row> {
    if values.explicit_row {
        return Err("Explicit row is not supported".into());
    }

    if values.rows.len() != 1 {
        return Err("Expected exactly one row".into());
    }

    let mut row = Vec::new();
    let source = values.rows.into_iter().next().unwrap();
    for column in source {
        let value = match column {
            Expr::Value(value) => map_value(value)?,
            _ => return Err("Unsupported expression type (create_row)".into()),
        };

        row.push(value)
    }

    Ok(Row::from(row))
}

fn type_of(column: &ColumnDef) -> Result<Type> {
    match column.data_type {
        ast::DataType::Bool | ast::DataType::Boolean => Ok(Type::Bool),
        ast::DataType::Int(None) => Ok(Type::Integer),
        ast::DataType::Text => Ok(Type::Text),
        _ => Err("Unsupported column type".into()),
    }
}
