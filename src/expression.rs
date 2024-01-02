use sqlparser::ast::{BinaryOperator, Expr, Ident};

use crate::schema::Schema;
use crate::types::{Result, Row, Value};

pub enum Op {
    Add,
    Sub,
    Mul,
    Div,

    And,
    Or,

    Equal,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

pub enum Expression {
    Field(usize),
    Const(Value),

    BinOp(Box<Expression>, Op, Box<Expression>),
}

impl Expression {
    pub fn eval(&self, row: &Row) -> Result<Value> {
        match self {
            Expression::Field(index) => Ok(row.get(*index).clone()),
            Expression::Const(val) => Ok(val.clone()),
            Expression::BinOp(left, op, right) => {
                let left = left.eval(row)?;
                let right = right.eval(row)?;
                match op {
                    Op::Add => left.add(right),
                    Op::Sub => left.sub(right),
                    Op::Mul => left.mul(right),
                    Op::Div => left.div(right),

                    Op::And => left.and(right),
                    Op::Or => left.or(right),

                    Op::Equal => Ok(Value::Bool(left == right)),
                    Op::Less => Ok(Value::Bool(left < right)),
                    Op::LessOrEqual => Ok(Value::Bool(left <= right)),
                    Op::Greater => Ok(Value::Bool(left > right)),
                    Op::GreaterOrEqual => Ok(Value::Bool(left >= right)),
                }
            }
        }
    }

    pub fn parse(expr: Expr, schema: &Schema) -> Result<Self> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left = Expression::parse(*left, schema)?;
                let right = Expression::parse(*right, schema)?;
                let op = match op {
                    BinaryOperator::Plus => Op::Add,
                    BinaryOperator::Minus => Op::Sub,
                    BinaryOperator::Multiply => Op::Mul,
                    BinaryOperator::Divide => Op::Div,

                    BinaryOperator::And => Op::And,
                    BinaryOperator::Or => Op::Or,

                    BinaryOperator::Eq => Op::Equal,
                    BinaryOperator::Lt => Op::Less,
                    BinaryOperator::LtEq => Op::LessOrEqual,
                    BinaryOperator::Gt => Op::Greater,
                    BinaryOperator::GtEq => Op::GreaterOrEqual,

                    op => return Err(format!("Unsupported binary operation: {:?}", op).into()),
                };

                // TODO: typecheck?
                Ok(Expression::BinOp(Box::new(left), op, Box::new(right)))
            }
            Expr::Identifier(Ident {
                value,
                quote_style: None,
            }) => {
                let index = schema
                    .columns()
                    .position(|column| column.name == value)
                    .ok_or_else(|| format!("No such column: {}", value))?;
                Ok(Expression::Field(index))
            }
            Expr::Value(val) => {
                let val = Value::try_from(val)?;
                Ok(Expression::Const(val))
            }
            e => Err(format!("Unsupported expression kind: {:?}", e).into()),
        }
    }
}
