use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use rocksdb::{IteratorMode, Options, Transaction};
use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::expression::Expression;
use crate::ops::{self, Empty as EmptySource, Eval, Filter, FullScan, Operation, Sort};
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

    pub fn run(&self, program: Vec<ast::Statement>) -> Result<Output> {
        if program.len() != 1 {
            return Err("Cannot run more than one statement at time".into());
        }

        let statement = program.into_iter().next().unwrap();
        if self.log.load(Ordering::Relaxed) {
            println!("{:#?}", statement);
        }
        self.execute(statement)
    }

    fn execute(&self, statement: ast::Statement) -> Result<Output> {
        match statement {
            ast::Statement::CreateTable {
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
                hive_distribution: ast::HiveDistributionStyle::NONE,
                hive_formats:
                    Some(ast::HiveFormat {
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
            ast::Statement::Drop {
                object_type: ast::ObjectType::Table,
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
            ast::Statement::Query(query) => {
                let rows = self.query(*query)?;
                Ok(rows)
            }
            ast::Statement::Insert {
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
            } if after_columns.is_empty() => {
                self.insert(table_name, columns, *source)?;
                Ok(Output::Affected(1))
            }
            _ => Err("Not supported".into()),
        }
    }

    fn query(&self, query: ast::Query) -> Result<Output> {
        let (query, order_by) = match query {
            ast::Query {
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
            ast::SetExpr::Select(select) => self.select(*select, order_by),
            _ => Err("Unsupported query kind".into()),
        }
    }

    fn create(&self, name: ast::ObjectName, columns: Vec<ast::ColumnDef>) -> Result<()> {
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

    fn drop(&self, name: ast::ObjectName) -> Result<()> {
        let table = name.to_string();
        let transaction = self.db.transaction();
        transaction.delete(&table)?;
        transaction.commit()?;
        self.db.drop_cf(&table)?;
        self.tables.write().unwrap().remove(&table);
        Ok(())
    }

    fn insert(
        &self,
        name: ast::ObjectName,
        columns: Vec<ast::Ident>,
        source: ast::Query,
    ) -> Result<()> {
        let expr = match source {
            ast::Query {
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

        let rows = match expr {
            ast::SetExpr::Values(values) => create_row(values)?,
            _ => return Err("Unsupported insert expression kind".into()),
        };

        let table = name.to_string();
        let cf = self.db.cf_handle(&table).ok_or("No such table")?;

        let transaction = self.db.transaction();
        let table = self.get_table(table, &cf, &transaction)?;
        let schema = table.schema();
        let rows = if columns.is_empty() {
            rows
        } else {
            reorder(schema, columns, rows)?
        };

        for row in &rows {
            schema.check(row)?;
        }

        for row in rows {
            let key = table.get_key(&row);
            if transaction.get_for_update_cf(&cf, &key, true)?.is_some() {
                return Err("Entry with such primary key already exist".into());
            }

            let mut value = Vec::new();
            row.serialize(&mut value)?;
            transaction.put_cf(&cf, &key, value)?;
        }

        transaction.commit()?;
        Ok(())
    }

    fn select(&self, query: ast::Select, order_by: Vec<ast::OrderByExpr>) -> Result<Output> {
        let (table, expressions, selection) = match query {
            ast::Select {
                distinct: None,
                top: None,
                projection,
                into: None,
                from,
                lateral_views,
                selection,
                group_by: ast::GroupByExpr::Expressions(group_by_exprs),
                cluster_by,
                distribute_by,
                sort_by,
                having: None,
                named_window,
                qualify: None,
            } if from.len() <= 1
                && lateral_views.is_empty()
                && group_by_exprs.is_empty()
                && cluster_by.is_empty()
                && distribute_by.is_empty()
                && sort_by.is_empty()
                && named_window.is_empty() =>
            {
                let name = match from.into_iter().next() {
                    Some(ast::TableWithJoins {
                        relation:
                            ast::TableFactor::Table {
                                name,
                                alias: None,
                                args: None,
                                with_hints,
                                version: None,
                                partitions,
                            },
                        joins,
                    }) if joins.is_empty() && with_hints.is_empty() && partitions.is_empty() => {
                        Some(name.to_string())
                    }
                    None => None,
                    _ => return Err("Unsupported select source".into()),
                };

                (name, projection, selection)
            }
            _ => return Err("Unsupported select kind".into()),
        };

        let transaction = self.db.transaction();
        let mut source = match table {
            Some(table) => {
                let cf = self.db.cf_handle(&table).ok_or("No such table")?;
                let table = self.get_table(table, &cf, &transaction)?;
                let schema = table.schema().clone();

                let iter = transaction.iterator_cf(&cf, IteratorMode::Start);
                Box::new(FullScan::new(schema, iter)?) as Box<dyn Operation>
            }
            None => Box::new(EmptySource::new()) as Box<dyn Operation>,
        };

        if let Some(selection) = selection {
            let filter = Filter::new(selection, source)?;
            source = Box::new(filter)
        }

        // NOTE: this code expects that Sort operator does not alter row stream Schema, i.e. sort.schema() == source.schema()
        let (schema, expressions) = expand_select(expressions, source.schema())?;
        if !order_by.is_empty() {
            let sort = Sort::new(order_by, &expressions, source)?;
            source = Box::new(sort);
        }

        let mut source = Eval::new(expressions, schema, source)?;
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

fn create_row(values: ast::Values) -> Result<Vec<Row>> {
    if values.explicit_row {
        return Err("Explicit row is not supported".into());
    }

    let mut rows = Vec::new();
    for tuple in values.rows {
        let mut row = Vec::new();
        for column in tuple {
            let value = match column {
                ast::Expr::Value(value) => Value::try_from(value)?,
                _ => return Err("Unsupported expression type (create_row)".into()),
            };

            row.push(value)
        }

        rows.push(Row::from(row))
    }

    Ok(rows)
}

fn reorder(schema: &Schema, columns: Vec<ast::Ident>, mut rows: Vec<Row>) -> Result<Vec<Row>> {
    for row in rows.iter_mut() {
        if columns.len() != row.len() {
            return Err("Number of values does not match number of columns".into());
        }

        let mut values = Vec::with_capacity(row.len());
        for column in schema.columns() {
            let index = columns
                .iter()
                .position(|c| c.value == column.name)
                .ok_or_else(|| format!("Unknown column {}", column.name))?;
            values.push(row.get(index).clone());
        }

        *row = Row::from(values)
    }

    Ok(rows)
}

fn expand_select(
    exprs: Vec<ast::SelectItem>,
    schema: &Schema,
) -> Result<(Schema, Vec<Expression>)> {
    use crate::schema::Column;

    let mut columns = Vec::with_capacity(exprs.len());
    let mut expressions = Vec::with_capacity(exprs.len());
    for item in exprs {
        match item {
            ast::SelectItem::Wildcard(ast::WildcardAdditionalOptions {
                opt_except: None,
                opt_exclude: None,
                opt_rename: None,
                opt_replace: None,
            }) => {
                for (i, column) in schema.columns().enumerate() {
                    columns.push(column.clone());
                    expressions.push(Expression::Field(i));
                }
            }
            ast::SelectItem::UnnamedExpr(expr) => {
                let e = Expression::parse(expr, schema)?;
                columns.push(Column {
                    name: "?column?".into(),
                    type_: e.result_type(schema)?,
                });
                expressions.push(e);
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                let e = Expression::parse(expr, schema)?;
                columns.push(Column {
                    name: alias.to_string(),
                    type_: e.result_type(schema)?,
                });
                expressions.push(e);
            }
            _ => return Err("Unsupported projection type".into()),
        }
    }

    let schema = Schema {
        primary_key: None,
        columns,
    };

    Ok((schema, expressions))
}
