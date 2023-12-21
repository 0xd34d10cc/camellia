use std::path::Path;
use std::{error::Error, fmt::Display};

use rocksdb::{IteratorMode, Options, Transaction};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    self, ColumnDef, Expr, GroupByExpr, HiveDistributionStyle, HiveFormat, ObjectName, ObjectType,
    Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Values,
    WildcardAdditionalOptions,
};

use crate::types::{Type, Value};

type Database = rocksdb::TransactionDB<rocksdb::MultiThreaded>;

type Row = Vec<Value>;

pub struct RowSet {
    schema: Schema,
    rows: Vec<Row>,
}

impl Display for RowSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use comfy_table::Table;

        let mut table = Table::new();
        let header: Vec<_> = self
            .schema
            .columns
            .iter()
            .map(|field| field.name.clone())
            .collect();
        table.set_header(header);
        for row in &self.rows {
            let row: Vec<_> = row.iter().map(|value| value.to_string()).collect();
            table.add_row(row);
        }

        write!(f, "{}", table)
    }
}

#[derive(Serialize, Deserialize)]
struct Schema {
    columns: Vec<ColumnDef>,
}

impl Schema {
    fn check(&self, row: &[Value]) -> Result<usize, Box<dyn Error>> {
        if row.len() != self.columns.len() {
            return Err(format!(
                "number of fields does not match: expected {} but got {}",
                self.columns.len(),
                row.len()
            )
            .into());
        }

        let mut primary_key = None;
        for (i, (column, value)) in self.columns.iter().zip(row).enumerate() {
            let value_type = value.type_();
            let column_type = type_of(column)?;
            if value_type != column_type {
                return Err(format!(
                    "{} field type does not match: expected {} but got {}",
                    column.name, column_type, value_type
                )
                .into());
            }

            if is_primary_key(column) && primary_key.replace(i).is_some() {
                return Err("Duplicate primary key".into());
            }
        }

        let i = primary_key.ok_or("No primary key in schema")?;
        Ok(i)
    }
}

pub struct Engine {
    db: Database,
}

impl Engine {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
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

    pub fn run(&self, program: Vec<Statement>) -> Result<Option<RowSet>, Box<dyn Error>> {
        println!("{:#?}", program);
        if program.len() != 1 {
            return Err("Cannot run more than one statement at time".into());
        }

        let statement = program.into_iter().next().unwrap();
        self.execute(statement)
    }

    fn execute(&self, statement: Statement) -> Result<Option<RowSet>, Box<dyn Error>> {
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
                Ok(None)
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
                Ok(None)
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
                Ok(None)
            }
            _ => Err("Not supported".into()),
        }
    }

    fn query(&self, query: Query) -> Result<Option<RowSet>, Box<dyn Error>> {
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

    fn create(&self, name: ObjectName, columns: Vec<ColumnDef>) -> Result<(), Box<dyn Error>> {
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

    fn drop(&self, name: ObjectName) -> Result<(), Box<dyn Error>> {
        let table = name.to_string();
        let transaction = self.db.transaction();
        transaction.delete(&table)?;
        transaction.commit()?;
        self.db.drop_cf(&table)?;
        Ok(())
    }

    fn insert(&self, name: ObjectName, source: Query) -> Result<(), Box<dyn Error>> {
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

        let values = match expr {
            SetExpr::Values(values) => create_row(values)?,
            _ => return Err("Unsupported insert expression kind".into()),
        };

        let table = name.to_string();
        let cf = self.db.cf_handle(&table).ok_or("No such table")?;

        let transaction = self.db.transaction();
        let schema = self.read_schema(&table, &transaction)?;
        let primary_key_idx = schema.check(&values)?;
        let key = match values[primary_key_idx] {
            Value::Int(val) => val.to_be_bytes(),
            _ => return Err("Unsupported primary key type".into()),
        };

        if transaction.get_for_update_cf(&cf, key, true)?.is_some() {
            return Err("Entry with such primary key already exist".into());
        }

        let row: Row = values;
        let value = bincode::serialize(&row)?;
        transaction.put_cf(&cf, key, value)?;
        transaction.commit()?;
        Ok(())
    }

    fn select(&self, query: Select) -> Result<Option<RowSet>, Box<dyn Error>> {
        let table = match query {
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
            } if projection.len() == 1
                && from.len() == 1
                && lateral_views.is_empty()
                && group_by_exprs.is_empty()
                && cluster_by.is_empty()
                && distribute_by.is_empty()
                && sort_by.is_empty()
                && named_window.is_empty() =>
            {
                match projection.into_iter().next().unwrap() {
                    SelectItem::Wildcard(WildcardAdditionalOptions {
                        opt_except: None,
                        opt_exclude: None,
                        opt_rename: None,
                        opt_replace: None,
                    }) => {}
                    _ => return Err("Unsupported projection type".into()),
                }

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
                        name.to_string()
                    }
                    _ => return Err("Unsupported select source".into()),
                }
            }
            _ => return Err("Unusupported select kind".into()),
        };

        let cf = self.db.cf_handle(&table).ok_or("No such table")?;
        let transaction = self.db.transaction();
        let mut rowset = RowSet {
            schema: self.read_schema(&table, &transaction)?,
            rows: Vec::new(),
        };

        let mut iter = transaction.iterator_cf(&cf, IteratorMode::Start);
        loop {
            match iter.next() {
                Some(Ok((_, value))) => {
                    let row: Row = bincode::deserialize(&value)?;
                    rowset.rows.push(row);
                }
                Some(Err(e)) => return Err(e.into()),
                None => break Ok(Some(rowset)),
            }
        }
    }

    fn read_schema(
        &self,
        table: &str,
        transaction: &Transaction<'_, Database>,
    ) -> Result<Schema, Box<dyn Error>> {
        let bytes = transaction
            .get(table)?
            .ok_or("Schema for this table not found")?;
        let schema = bincode::deserialize(&bytes)?;
        Ok(schema)
    }
}

fn is_primary_key(column: &ColumnDef) -> bool {
    use sqlparser::ast::ColumnOption;

    column.options.iter().any(|option| matches!(option.option, ColumnOption::Unique { is_primary: true }))
}

fn map_value(value: ast::Value) -> Result<Value, Box<dyn Error>> {
    let value = match value {
        ast::Value::Boolean(val) => Value::Bool(val),
        ast::Value::Number(number, false) => Value::Int(number.parse::<i64>()?),
        ast::Value::SingleQuotedString(string) => Value::String(string),
        _ => return Err("Unsupported value type".into()),
    };

    Ok(value)
}

fn create_row(values: Values) -> Result<Row, Box<dyn Error>> {
    if values.explicit_row {
        return Err("Explicit row is not supported".into());
    }

    if values.rows.len() != 1 {
        return Err("Expected exactly one row".into());
    }

    let mut dst = Vec::new();
    let source = values.rows.into_iter().next().unwrap();
    for column in source {
        let value = match column {
            Expr::Value(value) => map_value(value)?,
            _ => return Err("Unsupported expression type (create_row)".into()),
        };

        dst.push(value)
    }

    Ok(dst)
}

fn type_of(column: &ColumnDef) -> Result<Type, Box<dyn Error>> {
    match column.data_type {
        ast::DataType::Bool | ast::DataType::Boolean => Ok(Type::Bool),
        ast::DataType::Int(None) => Ok(Type::Integer),
        ast::DataType::Text => Ok(Type::Text),
        _ => Err("Unsupported column type".into()),
    }
}
