use core::fmt;

use sqlparser::ast::{self, Function};

use crate::schema::{Schema, Type};
use crate::types::{Result, Row, Value};

#[derive(Debug, Clone, Copy)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,

    And,
    Or,

    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Op::Add => "+",
            Op::Sub => "-",
            Op::Mul => "*",
            Op::Div => "/",
            Op::And => "AND",
            Op::Or => "OR",
            Op::Equal => "=",
            Op::NotEqual => "!=",
            Op::Less => "<",
            Op::LessOrEqual => "<=",
            Op::Greater => ">",
            Op::GreaterOrEqual => ">=",
        };

        f.write_str(s)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Not,
    Plus,
    Minus,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            UnaryOp::Not => "NOT",
            UnaryOp::Plus => "+",
            UnaryOp::Minus => "-",
        };

        f.write_str(s)
    }
}

#[derive(Clone)]
pub enum Expression {
    Field(usize),
    Const(Value),

    Abs(Box<Expression>),
    UnaryOp(UnaryOp, Box<Expression>),
    BinOp(Box<Expression>, Op, Box<Expression>),
    Case(Vec<(Expression, Expression)>, Option<Box<Expression>>),
}

impl Expression {
    pub fn eval(&self, row: &Row) -> Result<Value> {
        match self {
            Expression::Field(index) => Ok(row.get(*index).clone()),
            Expression::Const(val) => Ok(val.clone()),
            Expression::UnaryOp(op, expr) => {
                let val = expr.eval(row)?;
                match op {
                    UnaryOp::Not => {
                        let val = val
                            .to_bool()
                            .ok_or("Cannot convert to BOOL for NOT operation")?;
                        Ok(Value::Bool(!val))
                    }
                    UnaryOp::Plus => {
                        // does not do anything
                        Ok(val)
                    }
                    UnaryOp::Minus => {
                        let val = val.to_int().ok_or("Cannot convert to INT for unary '-'")?;
                        Ok(Value::Int(-val))
                    }
                }
            }
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
                    Op::NotEqual => Ok(Value::Bool(left != right)),
                    Op::Less => Ok(Value::Bool(left < right)),
                    Op::LessOrEqual => Ok(Value::Bool(left <= right)),
                    Op::Greater => Ok(Value::Bool(left > right)),
                    Op::GreaterOrEqual => Ok(Value::Bool(left >= right)),
                }
            }
            Expression::Case(cases, otherwise) => {
                for (condition, result) in cases {
                    if condition.eval(row)?.to_bool().unwrap() {
                        return result.eval(row);
                    }
                }

                if let Some(otherwise) = otherwise {
                    otherwise.eval(row)
                } else {
                    // ¯\_(ツ)_/¯
                    // TODO: figure out the actual behavior
                    Ok(Value::Bool(false))
                }
            }
            Expression::Abs(arg) => {
                let val = arg
                    .eval(row)?
                    .to_int()
                    .ok_or("Cannot convert 'abs' arg to integer")?;

                Ok(Value::Int(val.abs()))
            }
        }
    }

    pub fn result_type(&self, schema: &Schema) -> Result<Type> {
        match self {
            Expression::Const(value) => Ok(value.type_()),
            Expression::Field(i) => {
                let column = schema
                    .columns
                    .get(*i)
                    .ok_or("Reference to unknown column")?;
                Ok(column.type_)
            }
            Expression::UnaryOp(op, expr) => {
                let t = expr.result_type(schema)?;
                match op {
                    UnaryOp::Not => {
                        if !t.convertable_to(Type::Bool) {
                            return Err(format!(
                                "Invalid NOT: cannot be applied to expression of type {t}"
                            )
                            .into());
                        }

                        Ok(Type::Bool)
                    }
                    UnaryOp::Plus => Ok(t),
                    UnaryOp::Minus => {
                        if !t.convertable_to(Type::Integer) {
                            return Err(format!(
                                "Invalid unary '-': cannot be applied to expression of type {t}"
                            )
                            .into());
                        }

                        Ok(Type::Integer)
                    }
                }
            }
            Expression::BinOp(left, op, right) => {
                let left = left.result_type(schema)?;
                let right = right.result_type(schema)?;
                match *op {
                    Op::Add | Op::Sub | Op::Mul | Op::Div => {
                        if !left.convertable_to(Type::Integer)
                            || !right.convertable_to(Type::Integer)
                        {
                            return Err(format!(
                                "Invalid {op}: operands ({left} and {right}) are not convertable to integer"
                            )
                            .into());
                        }

                        Ok(Type::Integer)
                    }
                    Op::And | Op::Or => {
                        if !left.convertable_to(Type::Bool) || !right.convertable_to(Type::Bool) {
                            return Err(format!(
                                "Invalid {op}: operands ({left} and {right}) are not convertable to integer"
                            )
                            .into());
                        }

                        Ok(Type::Bool)
                    }
                    Op::Equal
                    | Op::NotEqual
                    | Op::Greater
                    | Op::GreaterOrEqual
                    | Op::Less
                    | Op::LessOrEqual => {
                        if left != right {
                            return Err(format!("Attempt to compare values of different types ({left} and {right}) with {op}").into());
                        }

                        Ok(left)
                    }
                }
            }
            Expression::Abs(arg) => {
                let t = arg.result_type(schema)?;
                if !t.convertable_to(Type::Integer) {
                    return Err(format!(
                        "Cannot convert argument of 'abs' (type {}) to integer",
                        t
                    )
                    .into());
                }

                Ok(Type::Integer)
            }
            Expression::Case(cases, otherwise) => {
                let (_, result) = cases.first().expect("Empty case-when");
                let result_type = result.result_type(schema)?;
                for (c, r) in cases {
                    let c_type = c.result_type(schema)?;
                    let r_type = r.result_type(schema)?;

                    if !c_type.convertable_to(Type::Bool) {
                        return Err(format!(
                            "Cannot convert condition of type {} to bool (CASE-WHEN)",
                            c_type
                        )
                        .into());
                    }

                    if !r_type.convertable_to(result_type) {
                        return Err(format!(
                            "Cannot convert result of type {} to {} (CASE-WHEN)",
                            r_type, result_type
                        )
                        .into());
                    }
                }

                if let Some(otherwise) = otherwise {
                    let t = otherwise.result_type(schema)?;

                    if !t.convertable_to(result_type) {
                        return Err(format!(
                            "Cannot convert result of type {} to {} (CASE-WHEN)",
                            t, result_type
                        )
                        .into());
                    }
                }

                Ok(result_type)
            }
        }
    }

    pub fn parse(expr: ast::Expr, schema: &Schema) -> Result<Self> {
        match expr {
            ast::Expr::Function(Function {
                name,
                args,
                filter: None,
                null_treatment: None,
                over: None,
                distinct: false,
                special: false,
                order_by,
            }) if order_by.is_empty() => {
                let name = name.to_string().to_ascii_lowercase();
                if name != "abs" {
                    return Err(format!("Unknown function: {}", name).into());
                }

                if args.len() != 1 {
                    return Err(format!("Invalid number of arguments for {} function", name).into());
                }

                let arg = match args.into_iter().next().unwrap() {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => e,
                    _ => return Err("Unsupported function arg kind".into()),
                };

                let e = Expression::parse(arg, schema)?;
                Ok(Expression::Abs(Box::new(e)))
            }
            ast::Expr::Case {
                operand: None,
                conditions,
                results,
                else_result,
            } => {
                assert!(conditions.len() == results.len());
                let mut cases = Vec::with_capacity(conditions.len());

                for (condition, result) in conditions.into_iter().zip(results) {
                    let c = Expression::parse(condition, schema)?;
                    let r = Expression::parse(result, schema)?;
                    cases.push((c, r));
                }

                let otherwise = else_result
                    .map(|expr| Expression::parse(*expr, schema))
                    .transpose()?
                    .map(Box::new);
                Ok(Expression::Case(cases, otherwise))
            }
            ast::Expr::UnaryOp { op, expr } => {
                let e = Expression::parse(*expr, schema)?;
                let op = match op {
                    ast::UnaryOperator::Not => UnaryOp::Not,
                    ast::UnaryOperator::Plus => UnaryOp::Plus,
                    ast::UnaryOperator::Minus => UnaryOp::Minus,
                    _ => return Err(format!("Unsupported unary operator: {:?}", op).into()),
                };

                // Do a bit of const folding
                let e = match (op, e) {
                    (UnaryOp::Not, Expression::Const(Value::Bool(v))) => {
                        Expression::Const(Value::Bool(!v))
                    }
                    (UnaryOp::Plus, Expression::Const(v)) => Expression::Const(v),
                    (UnaryOp::Minus, Expression::Const(Value::Int(v))) => {
                        Expression::Const(Value::Int(-v))
                    }
                    (op, e) => Expression::UnaryOp(op, Box::new(e)),
                };

                Ok(e)
            }
            ast::Expr::BinaryOp { left, op, right } => {
                let left = Expression::parse(*left, schema)?;
                let right = Expression::parse(*right, schema)?;
                let op = match op {
                    ast::BinaryOperator::Plus => Op::Add,
                    ast::BinaryOperator::Minus => Op::Sub,
                    ast::BinaryOperator::Multiply => Op::Mul,
                    ast::BinaryOperator::Divide => Op::Div,

                    ast::BinaryOperator::And => Op::And,
                    ast::BinaryOperator::Or => Op::Or,

                    ast::BinaryOperator::Eq => Op::Equal,
                    ast::BinaryOperator::NotEq => Op::NotEqual,
                    ast::BinaryOperator::Lt => Op::Less,
                    ast::BinaryOperator::LtEq => Op::LessOrEqual,
                    ast::BinaryOperator::Gt => Op::Greater,
                    ast::BinaryOperator::GtEq => Op::GreaterOrEqual,

                    op => return Err(format!("Unsupported binary operation: {:?}", op).into()),
                };

                // TODO: typecheck?
                Ok(Expression::BinOp(Box::new(left), op, Box::new(right)))
            }
            ast::Expr::Nested(e) => Expression::parse(*e, schema),
            ast::Expr::Identifier(ast::Ident {
                value,
                quote_style: None,
            }) => {
                let index = schema
                    .columns()
                    .position(|column| column.name == value)
                    .ok_or_else(|| format!("No such column: {}", value))?;
                Ok(Expression::Field(index))
            }
            ast::Expr::Value(val) => {
                let val = Value::try_from(val)?;
                Ok(Expression::Const(val))
            }
            e => Err(format!("Unsupported expression kind: {:?}", e).into()),
        }
    }
}
