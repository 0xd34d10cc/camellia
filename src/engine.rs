use std::{fmt::Display, error::Error};

use rocksdb::{Options, IteratorMode, Transaction};
use serde::{Deserialize, Serialize};

use crate::query::{Field, Value, Query, self};

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
            .fields
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
    fields: Vec<Field>,
}

impl Schema {
    fn check(&self, row: &[Value]) -> Result<usize, Box<dyn Error>> {
        if row.len() != self.fields.len() {
            return Err(format!(
                "number of fields does not match: expected {} but got {}",
                self.fields.len(),
                row.len()
            )
            .into());
        }

        let mut primary_key = None;
        for (i, (field, value)) in self.fields.iter().zip(row).enumerate() {
            let value_type = value.type_();
            if !field.type_.can_hold(value_type) {
                return Err(format!(
                    "{} field type does not match: expected {} but got {}",
                    field.name, field.type_, value_type
                )
                .into());
            }

            if field.primary_key && primary_key.replace(i).is_some() {
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
    pub fn new(path: &str) -> Result<Self, Box<dyn Error>> {
        use rocksdb::TransactionDBOptions;
        let opts = Options::default();
        let txn_db_opts = TransactionDBOptions::default();
        let column_families = Database::list_cf(&opts, path)?;
        let db = Database::open_cf(&opts, &txn_db_opts, path, column_families)?;
        Ok(Engine { db })
    }

    pub fn run(&self, query: Query) -> Result<Option<RowSet>, Box<dyn Error>> {
        match query {
            Query::Create(create) => {
                self.create(create)?;
                Ok(None)
            }
            Query::Drop(drop) => {
                self.drop(drop)?;
                Ok(None)
            }
            Query::Insert(insert) => {
                self.insert(insert)?;
                Ok(None)
            }
            Query::Select(select) => {
                let rowset = self.select(select)?;
                Ok(rowset)
            }
        }
    }

    fn create(&self, query: query::Create) -> Result<(), Box<dyn Error>> {
        let primary_keys = query
            .fields
            .iter()
            .filter(|field| field.primary_key)
            .count();
        if primary_keys != 1 {
            return Err("Exactly one field of table must be marked as primary key".into());
        }

        let opts = Options::default();
        self.db.create_cf(&query.table, &opts)?;

        let transaction = self.db.transaction();
        if transaction.get(&query.table)?.is_some() {
            return Err("Table with such name already exist, but shouldn't".into());
        }

        let schema = Schema {
            fields: query.fields,
        };
        let schema = bincode::serialize(&schema)?;
        transaction.put(&query.table, schema)?;
        transaction.commit()?;
        Ok(())
    }

    fn drop(&self, query: query::Drop) -> Result<(), Box<dyn Error>> {
        let transaction = self.db.transaction();
        transaction.delete(&query.table)?;
        transaction.commit()?;
        self.db.drop_cf(&query.table)?;
        Ok(())
    }

    fn insert(&self, query: query::Insert) -> Result<(), Box<dyn Error>> {
        let cf = self.db.cf_handle(&query.table).ok_or("No such table")?;

        let transaction = self.db.transaction();
        let schema = self.read_schema(&query.table, &transaction)?;
        let primary_key_idx = schema.check(&query.values)?;
        let key = match query.values[primary_key_idx] {
            Value::Int(val) => val.to_be_bytes(),
            _ => return Err("Unsupported primary key type".into()),
        };

        if transaction.get_for_update_cf(&cf, key, true)?.is_some() {
            return Err("Entry with such primary key already exist".into());
        }

        let row: Row = query.values;
        let value = bincode::serialize(&row)?;
        transaction.put_cf(&cf, key, value)?;
        transaction.commit()?;
        Ok(())
    }

    fn select(&self, query: query::Select) -> Result<Option<RowSet>, Box<dyn Error>> {
        let cf = self.db.cf_handle(&query.table).ok_or("No such table")?;

        let transaction = self.db.transaction();
        let mut rowset = RowSet {
            schema: self.read_schema(&query.table, &transaction)?,
            rows: Vec::new(),
        };

        if let query::Selector::Fields(_) = query.selector {
            return Err("Projection is not implemented".into());
        }

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
