pub mod operations;
use operations::create::*;
use serde::{Deserialize, Serialize};
use sqlite3_ext::{
    ffi::SQLITE_NOTFOUND,
    query::ToParam,
    sqlite3_ext_main, sqlite3_ext_vtab,
    vtab::{
        ChangeInfo, ChangeType, ColumnContext, ConstraintOp, CreateVTab, IndexInfoConstraint,
        UpdateVTab, VTab, VTabConnection, VTabCursor,
    },
    Connection, FallibleIterator, FallibleIteratorMut, FromValue, Result as ExtResult, Value,
    ValueRef,
};

use std::{
    collections::HashMap,
    default,
    fmt::Display,
    ops::{
        Bound::{self},
        Deref, DerefMut,
    },
};

use crate::{
    utils::{
        aggregate_conditions_to_ranges, calculate_bucket, resolve_partition_name,
        validate_and_map_columns, Condition,
    },
    ConstraintOpDef, Lookup, PartitionAccessor, RangePartition, Root, Template,
};

use self::operations::update;

#[sqlite3_ext_main]
fn init(db: &Connection) -> ExtResult<()> {
    db.create_module("Partitioner", PartitionMetaTable::module(), ())?;
    Ok(())
}
#[sqlite3_ext_vtab(StandardModule, UpdateVTab)]
pub struct PartitionMetaTable<'vtab> {
    partition: RangePartition,
    connection: &'vtab Connection,
}
impl<'vtab> CreateVTab<'vtab> for PartitionMetaTable<'vtab> {
    fn create(
        db: &'vtab VTabConnection,
        _aux: &'vtab Self::Aux,
        args: &[&str],
    ) -> ExtResult<(String, PartitionMetaTable<'vtab>)>
    where
        Self: Sized,
    {
        let p: RangePartition = create_partition(db, args, true)?;
        let sql = p.get_template().create_table_query();
        Ok((
            sql.to_owned(),
            PartitionMetaTable {
                partition: p,
                connection: db,
            },
        ))
    }
    fn destroy(&mut self) -> ExtResult<()> {
        Ok(())
    }
}
impl<'vtab> UpdateVTab<'vtab> for PartitionMetaTable<'vtab> {
    fn update(&'vtab self, info: &mut ChangeInfo) -> ExtResult<i64> {
        let _t = match info.change_type() {
            ChangeType::Insert => "insert",
            ChangeType::Update => "update",
            ChangeType::Delete => "delete",
        };

        let (sql, params) = update(&self.partition, &self.connection, info)?;

        Ok(self.connection.execute(&sql, params)?)
    }
}
#[derive(Serialize, Deserialize, Debug)]
struct WhereClause {
    column_name: String,
    #[serde(with = "ConstraintOpDef")]
    operator: ConstraintOp,
    // #[serde(with = "ValueDef")]
    // right_hand_value: Option<Value>,
    constraint_index: i32,
}
impl WhereClause {
    fn get_name(&self) -> String {
        self.column_name.clone()
    }
    fn _get_operator(&self) -> ConstraintOp {
        self.operator
    }
    fn _get_constraint_index(&self) -> i32 {
        self.constraint_index
    }
}
#[derive(Serialize, Deserialize, Debug)]
struct WhereClauses(HashMap<String, Vec<WhereClause>>);
impl Deref for WhereClauses {
    type Target = HashMap<String, Vec<WhereClause>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WhereClauses {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl FromIterator<(String, Vec<WhereClause>)> for WhereClauses {
    fn from_iter<T: IntoIterator<Item = (String, Vec<WhereClause>)>>(iter: T) -> Self {
        let mut data: HashMap<String, Vec<WhereClause>> = HashMap::new();

        for (key, clauses) in iter {
            for clause in clauses {
                data.entry(key.to_string())
                    .or_insert_with(Vec::new)
                    .push(clause);
            }
        }

        WhereClauses(data)
    }
}
impl Display for WhereClause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.column_name,
            ConstraintOpDef::from(self.operator),
            "?"
        )
    }
}

fn construct_where_clause(
    index_info: &sqlite3_ext::vtab::IndexInfo,
    partition: &RangePartition,
) -> ExtResult<WhereClauses> {
    let mut column_name_map: HashMap<String, Vec<(IndexInfoConstraint, i32)>> = HashMap::new();
    for (index, constraint) in index_info
        .constraints()
        .enumerate()
        .filter(|(_index, c)| c.usable())
    {
        let column_name = partition.columns[constraint.column() as usize]
            .get_name()
            .to_owned();
        column_name_map
            .entry(column_name)
            .or_default()
            .push((constraint, index as i32));
    }

    let where_clauses = column_name_map
        .iter()
        .map(|(column_name, constraints)| {
            let clauses = constraints
                .iter()
                .map(|(constraint, index)| WhereClause {
                    column_name: column_name.into(),
                    operator: constraint.op(),
                    constraint_index: *index,
                })
                .collect::<Vec<WhereClause>>();
            ("partition_table".to_string(), clauses)
        })
        .collect();
    Ok(where_clauses)
}
impl<'vtab> VTab<'vtab> for PartitionMetaTable<'vtab> {
    type Aux = ();
    type Cursor = RangePartitionCursor<'vtab>;

    fn connect(
        db: &'vtab VTabConnection,
        _aux: &'vtab Self::Aux,

        args: &[&str],
    ) -> ExtResult<(String, PartitionMetaTable<'vtab>)>
    where
        Self: Sized,
    {
        let p = create_partition(db, args, false)?;
        let connection = db;

        Ok((
            p.get_template().create_table_query().to_owned(),
            PartitionMetaTable {
                partition: p,
                connection,
            },
        ))
    }
    fn open(&'vtab self) -> ExtResult<Self::Cursor> {
        println!("{}", self.partition.get_root().get_interval());
        println!("buckets: {}", "placeholder");
        Ok(RangePartitionCursor {
            rowid: 0,
            meta_table: &self,
            partition_tables: Vec::default(),
            rows: Vec::new(),
        })
    }

    fn best_index(&self, index_info: &mut sqlite3_ext::vtab::IndexInfo) -> ExtResult<()> {
        let mut argv_index = 0;
        for mut constraint in index_info.constraints() {
            if constraint.usable() {
                constraint.set_argv_index(Some(argv_index));
                argv_index += 1;
            }
        }
        println!("{:#?}", index_info);
        index_info.set_estimated_cost(1.0); // Set a default cost, could be refined.
        let mut where_clauses = construct_where_clause(index_info, &self.partition)?;
        let partition_column =
            where_clauses.get_key_value(&self.partition.get_root().partition_column);
        let lookup_where_clause = match partition_column {
            Some((_name, constraints)) => constraints
                .iter()
                .map(|constraint| {
                    let wherec = WhereClause {
                        column_name: "partition_value".to_string(),
                        operator: constraint.operator,
                        constraint_index: constraint.constraint_index,
                    };
                    Some(wherec)
                })
                .collect::<Option<Vec<WhereClause>>>(),
            None => None,
        };

        lookup_where_clause
            .and_then(|clause| where_clauses.insert("lookup_table".to_string(), clause));
        index_info.set_index_str(Some(&ron::to_string(&where_clauses).unwrap()))?;

        Ok(())
    }
    fn disconnect(&mut self) -> ExtResult<()> {
        Ok(())
    }
}

pub struct RangePartitionCursor<'cursor> {
    rowid: i64,
    partition_tables: Vec<(i64, String)>,
    rows: Vec<Vec<(String, Value)>>,
    meta_table: &'cursor PartitionMetaTable<'cursor>,
}

impl<'cursor> VTabCursor<'cursor> for RangePartitionCursor<'cursor> {
    fn filter(
        &mut self,
        _idx_num: i32,
        idx_str: Option<&str>,
        args: &mut [&mut ValueRef],
    ) -> ExtResult<()> {
        let where_clauses_serialized = idx_str.unwrap_or("");
        let where_clauses: WhereClauses = ron::from_str(where_clauses_serialized).unwrap();
        let lookup_where = where_clauses.get("lookup_table");
        let partition_where = where_clauses.get("partition_table");

        let lookup_conditions = match lookup_where {
            Some(constraints) => constraints
                .iter()
                .map(|constraint| {
                    let value = match args[constraint.constraint_index as usize].to_owned() {
                        Ok(value) => value,
                        Err(_err) => return None,
                    };
                    Some(Condition {
                        column: constraint.get_name(),
                        operator: constraint.operator,
                        value,
                    })
                })
                .flatten()
                .collect::<Vec<Condition>>(),
            None => Vec::new(),
        };

        let ranges = aggregate_conditions_to_ranges(lookup_conditions);
        let (lower_bound, upper_bound) = match ranges.get("partition_value") {
            Some(bounds) => *bounds,
            None => (
                Bound::Unbounded as Bound<i64>,
                Bound::Unbounded as Bound<i64>,
            ),
        };

        self.partition_tables = self
            .meta_table
            .partition
            .get_lookup()
            .get_partitions_by_range(self.meta_table.connection, lower_bound, upper_bound)?;

        let mut partition_where_str: String = String::default();
        if let Some(vec) = partition_where {
            partition_where_str = format!(
                "WHERE {}",
                vec.iter()
                    .map(|clause| {
                        format!(
                            "{} {} {}",
                            clause.column_name,
                            ConstraintOpDef::from(clause.operator),
                            "?"
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(" AND ")
            );
        }

        for (_, pair) in self.partition_tables.iter().enumerate() {
            let partition_name = &pair.1;
            let sql = format!("SELECT * FROM {} {}", partition_name, partition_where_str);
            let mut stmt = self.meta_table.connection.prepare(&sql)?;
            for (index, arg) in args.iter_mut().enumerate() {
                arg.bind_param(&mut stmt, (index + 1) as i32)?;
            }
            println!("SQL: {}", sql);

            let rows = stmt
                .query(())?
                .map(|row| {
                    let column_count = row.len();
                    let mut columns: Vec<(String, Value)> = Vec::default();
                    for i in 0..column_count {
                        let name = row[i].name()?;
                        let value = row[i].to_owned()?;
                        columns.push((name.to_string(), value));
                    }
                    Ok(columns)
                })
                .collect::<Vec<Vec<(String, Value)>>>()?;
            for row in rows {
                self.rows.push(row)
            }
        }

        self.rowid = 0;
        Ok(())
    }

    fn next(&mut self) -> ExtResult<()> {
        self.rowid += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.rowid().unwrap() >= self.rows.len() as i64
    }

    fn column(&self, idx: usize, c: &ColumnContext) -> ExtResult<()> {
        let row = self.rows.get(self.rowid()? as usize).unwrap();
        let column = match row.get(idx) {
            Some(col) => Ok(col),
            None => Err(sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error parsing column ").to_string()),
            )),
        }?;
        c.set_result(column.1.to_owned())?;

        Ok(())
    }

    fn rowid(&self) -> ExtResult<i64> {
        Ok(self.rowid)
    }
}
