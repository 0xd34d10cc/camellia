use super::{Create, Drop, Field, Insert, Query, Select, Selector, Type, Value, Var};

use nom::branch::alt;
use nom::bytes::complete::{take_while, take_while1};
use nom::character::complete::char;
use nom::combinator::{complete, map};
use nom::multi::separated_list1;
use nom::sequence::{delimited, pair, preceded, terminated};

pub type Input<'a> = &'a str;
pub type ParseError<'a> = nom::error::VerboseError<Input<'a>>;
pub type Parsed<'a, O> = nom::IResult<Input<'a>, O, ParseError<'a>>;

// Reference: https://www.sqlite.org/lang.html
pub fn parse(input: Input) -> Result<Query, String> {
    use nom::error::convert_error;
    use nom::Err;

    match complete(terminated(query, query_end))(input) {
        Ok(("", query)) => Ok(query),
        Err(Err::Error(e) | Err::Failure(e)) => Err(convert_error(input, e)),
        other => panic!("Unahandled case: {:?}", other),
    }
}

fn query(input: Input) -> Parsed<Query> {
    alt((
        map(select, Query::Select),
        map(insert, Query::Insert),
        map(create, Query::Create),
        map(drop, Query::Drop),
    ))(input)
}

fn query_end(input: Input) -> Parsed<Input> {
    use nom::combinator::eof;
    preceded(key(";"), eof)(input)
}

fn select(input: Input) -> Parsed<Select> {
    let (input, selector) = preceded(ikey("select"), selector)(input)?;
    let (input, table) = preceded(ikey("from"), variable)(input)?;
    let select = Select { selector, table };
    Ok((input, select))
}

fn selector(input: Input) -> Parsed<Selector> {
    alt((
        map(key("*"), |_| Selector::All),
        map(separated_list1(key(","), variable), Selector::Fields),
    ))(input)
}

fn insert(input: Input) -> Parsed<Insert> {
    let (input, table) = preceded(pair(ikey("insert"), ikey("into")), variable)(input)?;
    let (input, values) = preceded(ikey("values"), delimited(key("("), values, key(")")))(input)?;
    let insert = Insert { table, values };
    Ok((input, insert))
}

fn values(input: Input) -> Parsed<Vec<Value>> {
    separated_list1(key(","), value)(input)
}

fn value(input: Input) -> Parsed<Value> {
    alt((
        map(str_lit, Value::String),
        map(integer, Value::Int),
        map(bool, Value::Bool),
    ))(input)
}

fn bool(input: Input) -> Parsed<bool> {
    use nom::combinator::value;

    alt((value(true, key("true")), value(false, key("false"))))(input)
}

fn integer(input: Input) -> Parsed<i64> {
    preceded(spaces, nom::character::complete::i64)(input)
}

fn str_lit(input: Input) -> Parsed<String> {
    use nom::character::complete::none_of;
    use nom::multi::fold_many0;

    let str_internals = fold_many0(none_of("'"), String::new, |mut s, c| {
        s.push(c);
        s
    });
    delimited(key("'"), str_internals, char('\''))(input)
}

fn create(input: Input) -> Parsed<Create> {
    let (input, table) = preceded(pair(ikey("create"), ikey("table")), variable)(input)?;
    let (input, fields) = delimited(key("("), fields, key(")"))(input)?;
    let create = Create { table, fields };
    Ok((input, create))
}

fn fields(input: Input) -> Parsed<Vec<Field>> {
    separated_list1(key(","), field)(input)
}

fn field(input: Input) -> Parsed<Field> {
    let (input, name) = variable(input)?;
    let (input, type_) = type_(input)?;
    let field = Field { name, type_ };
    Ok((input, field))
}

fn type_(input: Input) -> Parsed<Type> {
    use nom::combinator::{value, verify};

    let varchar = delimited(key("varchar("), integer, key(")"));
    alt((
        value(Type::Bool, key("bool")),
        value(Type::Integer, key("int")),
        map(verify(varchar, |&size| size > 0), |size| {
            Type::Varchar(size as usize)
        }),
    ))(input)
}

fn drop(input: Input) -> Parsed<Drop> {
    let (input, table) = preceded(pair(ikey("drop"), ikey("table")), variable)(input)?;
    let drop = Drop { table };
    Ok((input, drop))
}

fn variable(input: Input) -> Parsed<Var> {
    preceded(spaces, identifier)(input)
}

fn identifier(input: Input) -> Parsed<Var> {
    let (input, first) = take_while1(|c: char| c.is_alphabetic() || c == '_')(input)?;
    let (input, second) = take_while(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    let name = [first, second].concat();
    let name = Var::from(name.as_str());
    Ok((input, name))
}

fn spaces(input: Input) -> Parsed<Input> {
    nom::character::complete::multispace0(input)
}

fn key<'a>(key: Input<'a>) -> impl FnMut(Input<'a>) -> Parsed<Input> {
    nom::sequence::preceded(spaces, nom::bytes::complete::tag(key))
}

fn ikey<'a>(key: Input<'a>) -> impl FnMut(Input<'a>) -> Parsed<Input> {
    nom::sequence::preceded(spaces, nom::bytes::complete::tag_no_case(key))
}

#[cfg(test)]
mod tests {
    use crate::query::{Create, Drop, Field, Insert, Query, Select, Selector, Type, Value};

    #[test]
    fn select_all() {
        let expected = Query::Select(Select {
            selector: Selector::All,
            table: "table".to_owned(),
        });

        let actual: Query = "select * from table;".parse().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn select_fields() {
        let expected = Query::Select(Select {
            selector: Selector::Fields(vec!["foo".to_owned(), "bar".to_owned(), "baz".to_owned()]),
            table: "table".to_owned(),
        });

        let actual: Query = "select foo, bar, baz from table;".parse().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn select_case_insensitive() {
        let expected = Query::Select(Select {
            selector: Selector::Fields(vec!["FoO".to_owned(), "bAr".to_owned(), "BaZ".to_owned()]),
            table: "table".to_owned(),
        });

        let actual: Query = "SeLeCt FoO, bAr, BaZ fRoM table;".parse().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_single() {
        let expected = Query::Insert(Insert {
            table: "table".to_owned(),
            values: vec![Value::Int(42)],
        });

        let actual: Query = "insert into table values (42);".parse().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_multiple() {
        let expected = Query::Insert(Insert {
            table: "table".to_owned(),
            values: vec![
                Value::String("kekus".to_owned()),
                Value::Int(69),
                Value::Bool(false),
            ],
        });

        let actual: Query = "insert into table values ('kekus', 69, false);"
            .parse()
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn create() {
        let expected = Query::Create(Create {
            table: "users".to_owned(),
            fields: vec![
                Field {
                    name: "id".to_owned(),
                    type_: Type::Integer,
                },
                Field {
                    name: "name".to_owned(),
                    type_: Type::Varchar(32),
                },
                Field {
                    name: "gender".to_owned(),
                    type_: Type::Bool,
                },
            ],
        });

        let actual: Query = "create table users (id int, name varchar(32), gender bool);"
            .parse()
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn drop() {
        let expected = Query::Drop(Drop {
            table: "users".to_owned(),
        });

        let actual: Query = "drop table users;".parse().unwrap();
        assert_eq!(actual, expected);
    }
}
