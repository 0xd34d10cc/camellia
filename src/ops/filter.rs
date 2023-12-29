use sqlparser::ast::{BinaryOperator, Expr, Ident};

use super::{Operation, Output};
use crate::types::{Result, Row, Schema, Value};

pub struct Filter<'a> {
    inner: Box<dyn Operation + 'a>,

    expr: FilterExpr,
}

impl<'a> Filter<'a> {
    pub fn new(selection: Expr, inner: Box<dyn Operation + 'a>) -> Result<Self> {
        let schema = inner.schema();
        let expr = parse(schema, selection)?;
        Ok(Filter { inner, expr })
    }
}

impl<'a> Operation for Filter<'a> {
    fn schema(&self) -> &crate::types::Schema {
        self.inner.schema()
    }

    fn poll(&mut self) -> crate::types::Result<super::Output> {
        match self.inner.poll()? {
            Output::Batch(mut batch) => {
                batch.retain(|row| self.expr.accepts(row));
                Ok(Output::Batch(batch))
            }
            Output::Finished => Ok(Output::Finished),
        }
    }
}

enum Operand {
    Immediate(Value),
    Field(usize /* index */),
}

impl Operand {
    fn get<'a>(&'a self, row: &'a Row) -> &'a Value {
        match self {
            Operand::Field(i) => row.get(*i),
            Operand::Immediate(value) => value,
        }
    }
}

enum FilterExpr {
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),

    Equal(Operand, Operand),
    Less(Operand, Operand),
    LessOrEqual(Operand, Operand),
    Greater(Operand, Operand),
    GreaterOrEqual(Operand, Operand),
}

impl FilterExpr {
    fn accepts(&self, row: &Row) -> bool {
        match self {
            FilterExpr::And(left, right) => left.accepts(row) && right.accepts(row),
            FilterExpr::Or(left, right) => left.accepts(row) || right.accepts(row),
            FilterExpr::Equal(left, right) => left.get(row) == right.get(row),
            FilterExpr::Less(left, right) => left.get(row) < right.get(row),
            FilterExpr::LessOrEqual(left, right) => left.get(row) <= right.get(row),
            FilterExpr::Greater(left, right) => left.get(row) > right.get(row),
            FilterExpr::GreaterOrEqual(left, right) => left.get(row) >= right.get(row),
        }
    }
}

enum Part {
    Expr(FilterExpr),
    Op(Operand),
}

fn parse(schema: &Schema, expr: Expr) -> Result<FilterExpr> {
    match parse_part(schema, expr)? {
        Part::Expr(filter) => Ok(filter),
        Part::Op(_) => Err("Invalid filter".into()),
    }
}

fn parse_part(schema: &Schema, expr: Expr) -> Result<Part> {
    let e = match expr {
        Expr::BinaryOp { left, op, right } => {
            let left = parse_part(schema, *left)?;
            let right = parse_part(schema, *right)?;
            match (op, left, right) {
                (BinaryOperator::And, Part::Expr(left), Part::Expr(right)) => {
                    Part::Expr(FilterExpr::And(Box::new(left), Box::new(right)))
                }
                (BinaryOperator::Or, Part::Expr(left), Part::Expr(right)) => {
                    Part::Expr(FilterExpr::Or(Box::new(left), Box::new(right)))
                }
                (BinaryOperator::Eq, Part::Op(left), Part::Op(right)) => {
                    Part::Expr(FilterExpr::Equal(left, right))
                }
                (BinaryOperator::Lt, Part::Op(left), Part::Op(right)) => {
                    Part::Expr(FilterExpr::Less(left, right))
                }
                (BinaryOperator::LtEq, Part::Op(left), Part::Op(right)) => {
                    Part::Expr(FilterExpr::LessOrEqual(left, right))
                }
                (BinaryOperator::Gt, Part::Op(left), Part::Op(right)) => {
                    Part::Expr(FilterExpr::Greater(left, right))
                }
                (BinaryOperator::GtEq, Part::Op(left), Part::Op(right)) => {
                    Part::Expr(FilterExpr::GreaterOrEqual(left, right))
                }
                _ => return Err("Invalid where clause".into()),
            }
        }
        Expr::Identifier(Ident {
            value,
            quote_style: None,
        }) => {
            let index = schema
                .columns
                .iter()
                .position(|column| column.name.value == value)
                .ok_or_else(|| format!("No such column: {}", value))?;

            Part::Op(Operand::Field(index))
        }
        Expr::Value(val) => {
            let val = Value::try_from(val)?;
            Part::Op(Operand::Immediate(val))
        }
        e => return Err(format!("Unsupported expression kind: {:?}", e).into()),
    };

    Ok(e)
}
