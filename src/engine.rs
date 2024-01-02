use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use rocksdb::{IteratorMode, Options, Transaction};
use sqlparser::ast::{
    ColumnDef, Expr, GroupByExpr, HiveDistributionStyle, HiveFormat, ObjectName, ObjectType,
    OrderByExpr, Query, Select, SetExpr, Statement, TableFactor, TableWithJoins, Values,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::ops::{self, Filter, FullScan, Operation, Projection, Sort};
use crate::schema::Schema;
use crate::table::Table;
use crate::types::{Database, Result, Row, RowSet, Value};

type ColumnFamily<'db> = Arc<rocksdb::BoundColumnFamily<'db>>;

pub enum Output {
    Rows(RowSet),
    Affected(usize),
}

pub struct Engine {
    db: Database,
    log: AtomicBool,

    tables: RwLock<HashMap<String, Arc<Table>>>,
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
        let tables = RwLock::new(HashMap::new());
        Ok(Engine {
            db,
            tables,
            log: AtomicBool::new(false),
        })
    }

    pub fn set_log(&self, on: bool) {
        self.log.store(on, Ordering::Relaxed);
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
        if self.log.load(Ordering::Relaxed) {
            println!("{:#?}", statement);
        }
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
                let name = names.into_iter().next().unwrap();
                self.drop(name)?;
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
        let (query, order_by) = match query {
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
            } if limit_by.is_empty() && locks.is_empty() => (*body, order_by),
            _ => return Err("Not implemented".into()),
        };

        match query {
            SetExpr::Select(select) => self.select(*select, order_by),
            _ => Err("Unsupported query kind".into()),
        }
    }

    fn create(&self, name: ObjectName, columns: Vec<ColumnDef>) -> Result<()> {
        let table = name.to_string();
        for column in &columns {
            crate::types::type_of(column)?;
        }

        let opts = Options::default();
        self.db.create_cf(&table, &opts)?;

        let transaction = self.db.transaction();
        if transaction.get(&table)?.is_some() {
            return Err("Table with such name already exist, but shouldn't".into());
        }

        let schema = Schema::new(columns)?;
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
        let table = self.get_table(table, &cf, &transaction)?;
        table.schema().check(&row)?;
        let key = table.get_key(&row);
        if transaction.get_for_update_cf(&cf, &key, true)?.is_some() {
            return Err("Entry with such primary key already exist".into());
        }

        let mut value = Vec::new();
        row.serialize(&mut value)?;
        transaction.put_cf(&cf, &key, value)?;
        transaction.commit()?;
        Ok(())
    }

    fn select(&self, query: Select, order_by: Vec<OrderByExpr>) -> Result<Output> {
        let (table, projection, selection) = match query {
            Select {
                distinct: None,
                top: None,
                projection,
                into: None,
                from,
                lateral_views,
                selection,
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
                        (name.to_string(), projection, selection)
                    }
                    _ => return Err("Unsupported select source".into()),
                }
            }
            _ => return Err("Unsupported select kind".into()),
        };

        let cf = self.db.cf_handle(&table).ok_or("No such table")?;
        let transaction = self.db.transaction();
        let table = self.get_table(table, &cf, &transaction)?;
        let schema = table.schema().clone();

        let iter = transaction.iterator_cf(&cf, IteratorMode::Start);
        let mut source = Box::new(FullScan::new(schema, iter)?) as Box<dyn Operation>;
        if let Some(selection) = selection {
            let filter = Filter::new(selection, source)?;
            source = Box::new(filter)
        }

        if !order_by.is_empty() {
            let sort = Sort::new(order_by, source)?;
            source = Box::new(sort);
        }

        let mut source = Projection::new(&projection, source)?;
        let schema = source.schema().clone();
        let mut rows = Vec::new();
        loop {
            match source.poll() {
                Ok(ops::Output::Finished) => break Ok(Output::Rows(RowSet { rows, schema })),
                Ok(ops::Output::Batch(mut batch)) => {
                    rows.append(&mut batch);
                }
                Err(e) => break Err(e),
            }
        }
    }

    fn get_table(
        &self,
        table: String,
        cf: &ColumnFamily<'_>,
        transaction: &Transaction<'_, Database>,
    ) -> Result<Arc<Table>> {
        if let Some(table) = self.tables.read().unwrap().get(&table).cloned() {
            return Ok(table);
        }

        let schema = self.read_schema(&table, transaction)?;
        let hidden_pk = if schema.primary_key.is_none() {
            self.read_hidden_pk(cf, transaction)?
        } else {
            0
        };

        let t = Arc::new(Table::new(schema, hidden_pk));
        self.tables
            .write()
            .unwrap()
            .entry(table)
            .or_insert(t.clone());
        Ok(t)
    }

    fn read_schema(&self, table: &str, transaction: &Transaction<'_, Database>) -> Result<Schema> {
        let bytes = transaction
            .get(table)?
            .ok_or("Schema for this table not found")?;
        let schema = bincode::deserialize(&bytes)?;
        Ok(schema)
    }

    fn read_hidden_pk(
        &self,
        cf: &ColumnFamily<'_>,
        transaction: &Transaction<'_, Database>,
    ) -> Result<u64> {
        let mut iter = transaction.iterator_cf(cf, IteratorMode::End);
        match iter.next().transpose()? {
            Some((key, _value)) => {
                assert!(key.len() == 8);
                Ok(u64::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]))
            }
            None => Ok(0),
        }
    }
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
            Expr::Value(value) => Value::try_from(value)?,
            _ => return Err("Unsupported expression type (create_row)".into()),
        };

        row.push(value)
    }

    Ok(Row::from(row))
}
