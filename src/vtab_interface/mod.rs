pub mod operations;
use operations::create::*;
use sqlite3_ext::{
    ffi::SQLITE_NOTFOUND,
    sqlite3_ext_main, sqlite3_ext_vtab,
    vtab::{
        ChangeInfo, ChangeType, ColumnContext, CreateVTab, UpdateVTab, VTab, VTabConnection,
        VTabCursor,
    },
    Connection, FallibleIterator, FallibleIteratorMut, FromValue, Result as ExtResult, Value,
    ValueRef,
};

use std::ops::Bound::{self};

use crate::{
    utils::{
        aggregate_conditions_to_ranges, calculate_bucket, parse_conditions, resolve_partition_name,
        validate_and_map_columns,
    },
    ConstraintOperators, Lookup, PartitionAccessor, RangePartition, Root, Template,
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
fn construct_where_clause(
    index_info: &sqlite3_ext::vtab::IndexInfo,
    partition: &RangePartition,
) -> ExtResult<String> {
    let partition_column_name = &partition.root.partition_column;
    let partition_column_constraints = index_info.constraints().filter_map(|constraint| {
        let column_name = partition.columns[constraint.column() as usize].get_name();
        if column_name.to_uppercase() == partition_column_name.to_uppercase() && constraint.usable()
        {
            Some(constraint)
        } else {
            None
        }
    });

    let lookup_table_where_clauses: Result<Vec<String>, sqlite3_ext::Error> =
        partition_column_constraints
            .map(|constraint| {
                let bucket_value =
                    calculate_bucket(&constraint.rhs()?.to_owned()?, partition.interval)?;
                Ok(format!(
                    "partition_value {} {}",
                    ConstraintOperators(constraint.op()),
                    bucket_value,
                ))
            })
            .collect();

    Ok(lookup_table_where_clauses?.join(" AND "))
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
        index_info.set_estimated_cost(1.0); // Set a default cost, could be refined.

        let where_clause = construct_where_clause(index_info, &self.partition)?;
        println!("{}", where_clause);
        index_info.set_index_str(Some(&where_clause))?;

        Ok(())
    }
    fn disconnect(&mut self) -> ExtResult<()> {
        Ok(())
    }
}

// type PartitionColumnConstraint = (&str, )

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
        println!("number of arguments: {}", args.len());
        let range = parse_conditions(idx_str.unwrap_or(""));
        let range = aggregate_conditions_to_ranges(range);
        println!("{}", idx_str.unwrap_or("no idx str"));
        let partition_column_range: &(Bound<i64>, Bound<i64>) =
            range.get("partition_value").unwrap_or(&(
                Bound::Unbounded as Bound<i64>,
                Bound::Unbounded as Bound<i64>,
            ));

        let lower_bound = partition_column_range.0;
        let upper_bound = partition_column_range.1;

        self.partition_tables = self
            .meta_table
            .partition
            .get_lookup()
            .get_partitions_by_range(self.meta_table.connection, lower_bound, upper_bound)?;

        for (_, pair) in self.partition_tables.iter().enumerate() {
            let partition_name = &pair.1;

            let sql = format!("SELECT * FROM {}", partition_name);

            let rows = self
                .meta_table
                .connection
                .query(&sql, ())?
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
