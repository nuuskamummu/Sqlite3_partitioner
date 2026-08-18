pub mod operations;
mod vtab_cursor;
mod vtab_module;

use crate::constraints::{WhereClause, WhereClauses};
use crate::{
    cleanup::cleanup_expired_partitions, shadow_tables::interface::VirtualTable,
    vtab_interface::vtab_module::*,
};
use operations::create::*;
use sqlite3_ext::{
    ffi::SQLITE_NOTFOUND,
    function::{Context, FunctionOptions},
    sqlite3_ext_main,
    vtab::{ChangeInfo, IndexInfoConstraint},
    Connection, FromValue, Result as ExtResult, ValueRef,
};

use std::ops::Bound;
use std::{collections::HashMap, sync::RwLock};

use crate::utils::parse_to_unix_epoch;

/// Initializes the database with the Partitioner module.
///
/// This function sets up the virtual table module "Partitioner" in the SQLite database
/// to manage partitioned tables. It leverages a global lock for thread safety.
///
/// Parameters:
/// - `db`: Reference to the active database connection.
///
/// Returns:
/// - `ExtResult<()>`: Ok if successful, or an error on failure.
#[sqlite3_ext_main]
pub(crate) fn init(db: &Connection) -> ExtResult<()> {
    db.create_module(
        "Partitioner",
        PartitionMetaTable::module(),
        RwLock::default(),
    )?;
    let fn_opts = FunctionOptions::default().set_n_args(1);
    db.create_scalar_function(
        "partitioner_cleanup",
        &fn_opts,
        |ctx: &mut Context, args: &mut [&mut ValueRef]| {
            let table_name = args
                .get_mut(0)
                .ok_or_else(|| sqlite3_ext::Error::Module("Expected table name argument".into()))?
                .get_str()?;
            let count = cleanup_expired_partitions(ctx.db(), table_name)?;
            ctx.set_result(count)?;
            Ok(())
        },
    )?;
    let count_fn_opts = FunctionOptions::default().set_n_args(3);
    db.create_scalar_function(
        "partitioner_count_between",
        &count_fn_opts,
        |ctx: &mut Context, args: &mut [&mut ValueRef]| {
            let table_name = args
                .get_mut(0)
                .ok_or_else(|| sqlite3_ext::Error::Module("Expected table name argument".into()))?
                .get_str()?
                .to_string();
            let (start_epoch, end_epoch) = {
                let (_, range_args) = args.split_at_mut(1);
                let (start_args, end_args) = range_args.split_at_mut(1);
                let range_start = start_args.get_mut(0).ok_or_else(|| {
                    sqlite3_ext::Error::Module("Expected range start argument".into())
                })?;
                let range_end = end_args.get_mut(0).ok_or_else(|| {
                    sqlite3_ext::Error::Module("Expected range end argument".into())
                })?;
                parse_range_epochs(range_start, range_end)?
            };
            let table = VirtualTable::connect(ctx.db(), &table_name)?;
            let (lower_bound, upper_bound) =
                partition_aligned_bounds(start_epoch, end_epoch, table.partition_interval())?;
            let count = table.row_count_for_range(&lower_bound, &upper_bound)?;
            ctx.set_result(count)?;
            Ok(())
        },
    )?;
    Ok(())
}

fn parse_range_epochs(range_start: &ValueRef, range_end: &ValueRef) -> ExtResult<(i64, i64)> {
    let start_epoch = parse_to_unix_epoch(range_start)?;
    let end_epoch = parse_to_unix_epoch(range_end)?;
    if end_epoch <= start_epoch {
        return Err(sqlite3_ext::Error::Module(
            "Expected range end to be greater than range start".into(),
        ));
    }
    Ok((start_epoch, end_epoch))
}

fn partition_aligned_bounds(
    start_epoch: i64,
    end_epoch: i64,
    interval: i64,
) -> ExtResult<(Bound<i64>, Bound<i64>)> {
    if start_epoch.rem_euclid(interval) != 0 || end_epoch.rem_euclid(interval) != 0 {
        return Err(sqlite3_ext::Error::Module(
            "partitioner_count_between requires partition-aligned bounds".into(),
        ));
    }
    Ok((Bound::Included(start_epoch), Bound::Excluded(end_epoch)))
}

/// Constructs `WhereClauses` from the provided index information and virtual table.
///
/// This function parses the index information to generate SQL WHERE clauses that are
/// applicable for querying the virtual table, based on its column constraints and indexes.
/// Constraints whose argv index appears in `skip` (claimed by a companion-driven
/// scan) are excluded — the companion enforces them itself.
///
/// Parameters:
/// - `index_info`: Index information provided by the SQLite VTAB method bestIndex.
/// - `virtual_table`: Reference to the `VirtualTable`.
/// - `skip`: argv indexes of constraints claimed by companions.
///
/// Returns:
/// - A result containing `WhereClauses` if successful, or an error on failure.
fn construct_where_clause(
    index_info: &sqlite3_ext::vtab::IndexInfo,
    virtual_table: &VirtualTable,
    skip: &std::collections::HashSet<i32>,
) -> ExtResult<WhereClauses> {
    let mut column_name_map: HashMap<String, Vec<(IndexInfoConstraint, i32)>> = HashMap::new();
    // Index must match the argv_index assigned in best_index: a sequential counter over
    // accepted constraints, not the position in the full constraint list.
    let mut argv_index = 0i32;
    for constraint in index_info
        .constraints()
        .filter(|c| c.usable() && crate::vtab_interface::vtab_module::is_row_constraint(c))
    {
        let index = argv_index;
        argv_index += 1;
        if skip.contains(&index) {
            continue;
        }
        let column_name = virtual_table.columns().0[constraint.column() as usize]
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
                .map(|(constraint, index)| {
                    WhereClause::new(column_name.to_owned(), constraint.op(), *index)
                })
                .collect::<Vec<WhereClause>>();
            (
                virtual_table
                    .lookup()
                    .partition_table_column()
                    .get_name()
                    .to_owned(),
                clauses,
            )
        })
        .collect();
    Ok(where_clauses)
}

#[cfg(test)]
mod tests {

    use std::ops::{Index, IndexMut};

    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::{
        Connection, FallibleIterator, FallibleIteratorMut, FromValue, TransactionType,
    };

    use super::init;
    fn setup_db(rusq_conn: &RusqConn) -> &Connection {
        let conn = Connection::from_rusqlite(rusq_conn);
        conn
    }
    fn init_rusq_conn() -> RusqConn {
        RusqConn::open_in_memory().unwrap()
    }

    fn create_hourly_test_table(db: &Connection) -> sqlite3_ext::Result<()> {
        init(db)?;
        db.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)",
            (),
        )?;
        Ok(())
    }

    #[test]
    fn test_load_extension() {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
    }
    #[test]
    fn test_create_virtual_table() {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok())
    }
    #[test]
    fn test_create_virtual_table_no_partition_column() {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp, col2 text)";
        assert!(db.execute(sql, ()).is_err())
    }

    #[test]
    fn test_create_virtual_table_no_interval() {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_err())
    }
    #[test]
    fn test_created_root_table() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        db.query_row(
            "SELECT sql FROM sqlite_schema where name = 'test_root'",
            (),
            |result| {
                let result_query = result.index_mut(0).get_str()?;
                assert_eq!(
                    result_query,
                    "CREATE TABLE test_root (partition_column TEXT, partition_value INTEGER, lifetime INTEGER)"
                );
                Ok(())
            },
        )?;
        Ok(())
    }

    #[test]
    fn test_created_lookup_table() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        db.query_row(
            "SELECT sql FROM sqlite_schema where name = 'test_lookup'",
            (),
            |result| {
                let result_query = result.index_mut(0).get_str()?;
                assert_eq!(
                    result_query,
                    "CREATE TABLE test_lookup (partition_table TEXT UNIQUE, partition_value INTEGER UNIQUE, expires_at INTEGER)"
                );
                Ok(())
            },
        )?;
        Ok(())
    }
    #[test]
    fn test_created_template_table() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        db.query_row(
            "SELECT sql FROM sqlite_schema where name = 'test_template'",
            (),
            |result| {
                let result_query = result.index_mut(0).get_str()?;
                assert_eq!(
                    result_query,
                    "CREATE TABLE test_template (col1 TEXT, col2 TEXT)"
                );
                Ok(())
            },
        )?;
        Ok(())
    }

    #[test]
    fn test_created_stats_table() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        db.query_row(
            "SELECT sql FROM sqlite_schema where name = 'test_stats'",
            (),
            |result| {
                let result_query = result.index_mut(0).get_str()?;
                assert_eq!(
                    result_query,
                    "CREATE TABLE test_stats (partition_table TEXT UNIQUE, row_count INTEGER NOT NULL DEFAULT 0)"
                );
                Ok(())
            },
        )?;
        Ok(())
    }

    #[test]
    fn test_partitioner_count_between_uses_partition_stats() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_hourly_test_table(db)?;
        let txn = db.transaction(TransactionType::Immediate)?;
        txn.insert(
            "INSERT INTO test values ('2024-01-01 12:15', 'test string')",
            (),
        )?;
        txn.insert(
            "INSERT INTO test values ('2024-01-01 12:30', 'test string')",
            (),
        )?;
        txn.insert(
            "INSERT INTO test values ('2024-01-01 13:15', 'test string')",
            (),
        )?;
        txn.commit()?;

        db.query_row(
            "SELECT partitioner_count_between('test', '2024-01-01 12:00', '2024-01-01 14:00')",
            (),
            |row| {
                assert_eq!(row.index_mut(0).get_i64(), 3);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_partitioner_count_between_rejects_unaligned_bounds() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_hourly_test_table(db)?;
        db.insert(
            "INSERT INTO test values ('2024-01-01 12:15', 'test string')",
            (),
        )?;

        let result = db.query_row(
            "SELECT partitioner_count_between('test', '2024-01-01 12:15', '2024-01-01 13:00')",
            (),
            |row| Ok(row.index_mut(0).get_i64()),
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_insert() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        assert!(db
            .insert("INSERT INTO test values ('2024-01-01', 'test string')", ())
            .is_ok());

        Ok(())
    }

    #[test]
    fn test_insert_updates_partition_stats() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_hourly_test_table(db)?;
        let txn = db.transaction(sqlite3_ext::TransactionType::Immediate)?;
        txn.insert(
            "INSERT INTO test values ('2024-01-01 12:15', 'a'), ('2024-01-01 12:45', 'b'), ('2024-01-01 13:15', 'c')",
            (),
        )?;
        txn.commit()?;

        db.query_row(
            "SELECT row_count FROM test_stats WHERE partition_table = 'test_1704110400'",
            (),
            |res| {
                assert_eq!(res.index_mut(0).get_i64(), 2);
                Ok(())
            },
        )?;

        db.query_row(
            "SELECT row_count FROM test_stats WHERE partition_table = 'test_1704114000'",
            (),
            |res| {
                assert_eq!(res.index_mut(0).get_i64(), 1);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_insert_without_partition_column() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        assert!(db
            .insert("INSERT INTO test (col2) values ('test string')", ())
            .is_err());

        Ok(())
    }

    #[test]
    fn test_insert_only_partition_column() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        assert!(db
            .insert("INSERT INTO test (col1) values ('2024-02-01')", ())
            .is_ok());

        Ok(())
    }

    #[test]
    fn test_hourly_interval() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        assert!(db
            .insert(
                "INSERT INTO test (col1) values ('2024-02-01'),('2024-02-02 11:00'),('2024-02-02 12:00'),('2024-02-02 13:00'),('2024-02-02 14:00'),('2024-02-02 15:00'),('2024-02-02 15:30'),('2024-02-02 16:00'),('2024-02-02 17:00'),('2024-02-02 18:00')",
                ()
            )
            .is_ok());
        db.query_row("SELECT count(*) from test_lookup", (), |res| {
            let count = res.index(0).get_i64();
            assert_eq!(count, 9);
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn test_daily_interval() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 day, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        assert!(db
            .insert(
                "INSERT INTO test (col1) values ('2024-02-01'),('2024-02-02 11:00'),('2024-02-02 12:00'),('2024-02-02 13:00'),('2024-02-02 14:00'),('2024-02-02 15:00'),('2024-02-02 15:30'),('2024-02-02 16:00'),('2024-02-02 17:00'),('2024-02-02 18:00')",
                ()
            )
            .is_ok());
        db.query_row("SELECT count(*) from test_lookup", (), |res| {
            let count = res.index(0).get_i64();
            assert_eq!(count, 2);
            Ok(())
        })?;
        Ok(())
    }
    #[test]
    fn test_select() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 day, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        assert!(db
            .insert(
                "INSERT INTO test (col1) values ('2024-02-01'),('2024-02-03'),('2024-02-04'),('2024-02-05'),('2024-02-06'),('2024-02-07'),('2024-02-08'),('2024-02-09'),('2024-02-10'),('2024-02-11')",
                ()
            )
            .is_ok());
        db.query_row(
            "SELECT count(*) from test where col1 > '2024-02-10'",
            (),
            |res| {
                let count = res.index(0).get_i64();
                assert_eq!(count, 1);
                Ok(())
            },
        )?;

        db.query_row(
            "SELECT count(*) from test where col1 > '2024-02-10' or col1 < '2024-02-05'",
            (),
            |res| {
                let count = res.index(0).get_i64();
                assert_eq!(count, 4);
                Ok(())
            },
        )?;
        Ok(())
    }

    #[test]
    fn test_select_gt_mid_partition_keeps_boundary_partition() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_hourly_test_table(db)?;
        db.insert(
            "INSERT INTO test values
            ('2024-01-01 12:15', 'a'),
            ('2024-01-01 12:45', 'b'),
            ('2024-01-01 13:15', 'c')",
            (),
        )?;

        db.query_row(
            "SELECT count(*) FROM test WHERE col1 > '2024-01-01 12:30'",
            (),
            |res| {
                let count = res.index(0).get_i64();
                assert_eq!(count, 2);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_select_gte_exact_boundary_keeps_boundary_partition() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_hourly_test_table(db)?;
        db.insert(
            "INSERT INTO test values
            ('2024-01-01 12:00', 'a'),
            ('2024-01-01 12:30', 'b'),
            ('2024-01-01 13:00', 'c')",
            (),
        )?;

        db.query_row(
            "SELECT count(*) FROM test WHERE col1 >= '2024-01-01 12:00'",
            (),
            |res| {
                let count = res.index(0).get_i64();
                assert_eq!(count, 3);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_select_lt_exact_boundary_does_not_touch_next_partition() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_hourly_test_table(db)?;
        let txn = db.transaction(TransactionType::Immediate)?;
        txn.insert(
            "INSERT INTO test values
            ('2024-01-01 12:15', 'a'),
            ('2024-01-01 13:15', 'b')",
            (),
        )?;
        txn.commit()?;

        let next_partition_name = db.query_row(
            "SELECT partition_table FROM test_lookup WHERE partition_value = strftime('%s', '2024-01-01 13:00:00')",
            (),
            |res| Ok(res.index_mut(0).get_str()?.to_string()),
        )?;

        db.execute(&format!("DROP TABLE {}", next_partition_name), ())?;

        db.query_row(
            "SELECT count(*) FROM test WHERE col1 < '2024-01-01 13:00'",
            (),
            |res| {
                let count = res.index(0).get_i64();
                assert_eq!(count, 1);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_select_invalid_partition_predicate_returns_error() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_hourly_test_table(db)?;
        db.insert("INSERT INTO test values ('2024-01-01 12:15', 'a')", ())?;

        let result = db.query_row(
            "SELECT count(*) FROM test WHERE col1 > 'not-a-timestamp'",
            (),
            |res| Ok(res.index(0).get_i64()),
        );

        assert!(result.is_err());

        Ok(())
    }
    #[test]
    fn test_drop() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        let day_in_seconds = 86400;
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 day, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        assert!(db
            .insert(
                "INSERT INTO test (col1) values ('2024-02-01'),('2024-02-02'),('2024-02-03'),('2024-02-04'),('2024-02-05'),('2024-02-06'),('2024-02-07'),('2024-02-08'),('2024-02-09'),('2024-02-10')",
                ()
            )
            .is_ok());
        let mut rows = db.query(
            "SELECT partition_table from test_lookup order by partition_value asc",
            (),
        )?;
        let partition_names = rows
            .map(|row| Ok(row.index_mut(0).get_str()?.to_string()))
            .collect::<Vec<String>>()?;

        partition_names
            .iter()
            .enumerate()
            .for_each(|(index, name)| {
                assert_eq!(
                    name.to_owned(),
                    format!("test_{}", 1706745600 + (day_in_seconds * index))
                )
            });

        db.execute("DROP TABLE test", ())?;
        let rows = db.query(
            "SELECT partition_table from test_lookup order by partition_value asc",
            (),
        );
        assert!(rows.is_err());

        Ok(())
    }
    #[test]
    fn test_update() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql = "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        assert!(db
            .insert(
                "INSERT INTO test values ('2024-01-01 12:00', 'test string')",
                ()
            )
            .is_ok());
        assert!(db
            .insert(
                "INSERT INTO test values ('2024-01-01 14:00', 'test string')",
                ()
            )
            .is_ok());
        assert!(db
            .execute(
                "UPDATE test SET col2 = 'string test' WHERE col1 > '2024-01-01 13:00'",
                ()
            )
            .is_ok());

        db.query_row(
            "SELECT col2 from test WHERE col1 = '2024-01-01 14:00'",
            (),
            |res| {
                let col2 = res.index_mut(0).get_str()?;
                assert_eq!(col2, "string test");
                Ok(())
            },
        )?;
        db.query_row(
            "SELECT col2 from test WHERE col1 = '2024-01-01 12:00'",
            (),
            |res| {
                let col2 = res.index_mut(0).get_str()?;
                assert_eq!(col2, "test string");
                Ok(())
            },
        )?;
        Ok(())
    }

    #[test]
    fn test_update_partition_column_moves_row_and_updates_stats() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_hourly_test_table(db)?;
        db.insert("INSERT INTO test values ('2024-01-01 12:15', 'a')", ())?;

        db.execute(
            "UPDATE test SET col1 = '2024-01-01 13:15' WHERE col1 = '2024-01-01 12:15'",
            (),
        )?;

        db.query_row(
            "SELECT count(*) FROM test WHERE col1 = '2024-01-01 12:15'",
            (),
            |res| {
                assert_eq!(res.index_mut(0).get_i64(), 0);
                Ok(())
            },
        )?;

        db.query_row(
            "SELECT count(*) FROM test WHERE col1 = '2024-01-01 13:15'",
            (),
            |res| {
                assert_eq!(res.index_mut(0).get_i64(), 1);
                Ok(())
            },
        )?;

        db.query_row(
            "SELECT row_count FROM test_stats WHERE partition_table = 'test_1704110400'",
            (),
            |res| {
                assert_eq!(res.index_mut(0).get_i64(), 0);
                Ok(())
            },
        )?;

        db.query_row(
            "SELECT row_count FROM test_stats WHERE partition_table = 'test_1704114000'",
            (),
            |res| {
                assert_eq!(res.index_mut(0).get_i64(), 1);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_delete_updates_partition_stats() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_hourly_test_table(db)?;
        db.insert(
            "INSERT INTO test values ('2024-01-01 12:15', 'a'), ('2024-01-01 12:45', 'b')",
            (),
        )?;

        db.execute("DELETE FROM test WHERE col1 = '2024-01-01 12:15'", ())?;

        db.query_row(
            "SELECT row_count FROM test_stats WHERE partition_table = 'test_1704110400'",
            (),
            |res| {
                assert_eq!(res.index_mut(0).get_i64(), 1);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_cleanup_function() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        assert!(init(db).is_ok());
        let sql =
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)";
        assert!(db.execute(sql, ()).is_ok());
        assert!(db
            .insert(
                "INSERT INTO test values ('2024-01-01 12:00', 'test string')",
                ()
            )
            .is_ok());

        let partition_name = db.query_row(
            "SELECT partition_table FROM test_lookup LIMIT 1",
            (),
            |res| Ok(res.index_mut(0).get_str()?.to_string()),
        )?;

        db.execute("UPDATE test_lookup SET expires_at = 0", ())?;

        db.query_row("SELECT partitioner_cleanup('test')", (), |res| {
            let count = res.index_mut(0).get_i64();
            assert_eq!(count, 1);
            Ok(())
        })?;

        db.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name = ?",
            [partition_name.as_str()],
            |res| {
                let count = res.index_mut(0).get_i64();
                assert_eq!(count, 0);
                Ok(())
            },
        )?;

        db.query_row(
            "SELECT count(*) FROM test_lookup WHERE partition_table = ?",
            [partition_name.as_str()],
            |res| {
                let count = res.index_mut(0).get_i64();
                assert_eq!(count, 0);
                Ok(())
            },
        )?;

        db.query_row(
            "SELECT count(*) FROM test_stats WHERE partition_table = ?",
            [partition_name.as_str()],
            |res| {
                let count = res.index_mut(0).get_i64();
                assert_eq!(count, 0);
                Ok(())
            },
        )?;

        Ok(())
    }

    fn collect_col1_values(db: &Connection, sql: &str) -> sqlite3_ext::Result<Vec<String>> {
        let mut stmt = db.prepare(sql)?;
        let rows = stmt.query(())?;
        let mut values = Vec::new();
        while let Ok(Some(row)) = rows.next() {
            values.push(row.index_mut(0).get_str()?.to_string());
        }
        Ok(values)
    }

    fn create_out_of_order_test_table(db: &Connection) -> sqlite3_ext::Result<()> {
        init(db)?;
        db.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)",
            (),
        )?;
        // Out-of-order rows across three hourly partitions.
        db.execute(
            "INSERT INTO test (col1, col2) values
                ('2024-02-01 13:30', 'e'),
                ('2024-02-01 12:45', 'b'),
                ('2024-02-01 14:10', 'j'),
                ('2024-02-01 12:15', 'a'),
                ('2024-02-01 13:05', 'd'),
                ('2024-02-01 14:05', 'i')",
            (),
        )?;
        Ok(())
    }

    #[test]
    fn test_partition_column_index_propagates_to_partitions() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_out_of_order_test_table(db)?;
        // One index on the template itself...
        db.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'test_template' AND sql LIKE '%(col1)%'",
            (),
            |res| {
                assert_eq!(res.index(0).get_i64(), 1);
                Ok(())
            },
        )?;
        // ...and one inherited index on each of the three partitions.
        db.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND tbl_name LIKE 'test\\_1%' ESCAPE '\\'",
            (),
            |res| {
                assert_eq!(res.index(0).get_i64(), 3);
                Ok(())
            },
        )?;
        Ok(())
    }

    #[test]
    fn test_order_by_partition_column_asc() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_out_of_order_test_table(db)?;
        let values = collect_col1_values(db, "SELECT col1 FROM test ORDER BY col1 ASC")?;
        assert_eq!(
            values,
            vec![
                "2024-02-01 12:15",
                "2024-02-01 12:45",
                "2024-02-01 13:05",
                "2024-02-01 13:30",
                "2024-02-01 14:05",
                "2024-02-01 14:10",
            ]
        );
        Ok(())
    }

    #[test]
    fn test_order_by_partition_column_desc_with_limit() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_out_of_order_test_table(db)?;
        let values = collect_col1_values(db, "SELECT col1 FROM test ORDER BY col1 DESC LIMIT 2")?;
        assert_eq!(values, vec!["2024-02-01 14:10", "2024-02-01 14:05"]);
        Ok(())
    }

    #[test]
    fn test_order_by_partition_column_desc() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_out_of_order_test_table(db)?;
        let values = collect_col1_values(db, "SELECT col1 FROM test ORDER BY col1 DESC")?;
        assert_eq!(
            values,
            vec![
                "2024-02-01 14:10",
                "2024-02-01 14:05",
                "2024-02-01 13:30",
                "2024-02-01 13:05",
                "2024-02-01 12:45",
                "2024-02-01 12:15",
            ]
        );
        Ok(())
    }

    #[test]
    fn test_order_by_partition_column_with_range_filter() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_out_of_order_test_table(db)?;
        let values = collect_col1_values(
            db,
            "SELECT col1 FROM test WHERE col1 >= '2024-02-01 12:30' AND col1 < '2024-02-01 14:00' ORDER BY col1",
        )?;
        assert_eq!(
            values,
            vec!["2024-02-01 12:45", "2024-02-01 13:05", "2024-02-01 13:30"]
        );
        Ok(())
    }

    #[test]
    fn test_order_by_non_partition_column_still_sorted() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_out_of_order_test_table(db)?;
        let values = collect_col1_values(db, "SELECT col2 FROM test ORDER BY col2 DESC")?;
        assert_eq!(values, vec!["j", "i", "e", "d", "b", "a"]);
        Ok(())
    }

    fn explain_query_plan_has_sort(db: &Connection, sql: &str) -> sqlite3_ext::Result<bool> {
        let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {}", sql))?;
        let rows = stmt.query(())?;
        while let Ok(Some(row)) = rows.next() {
            let detail = row.index_mut(3).get_str()?;
            if detail.contains("USE TEMP B-TREE FOR ORDER BY") {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[test]
    fn test_order_by_partition_column_avoids_temp_btree() -> sqlite3_ext::Result<()> {
        let rusq_conn = init_rusq_conn();
        let db = setup_db(&rusq_conn);
        create_out_of_order_test_table(db)?;
        assert!(!explain_query_plan_has_sort(
            db,
            "SELECT col1 FROM test ORDER BY col1"
        )?);
        assert!(explain_query_plan_has_sort(
            db,
            "SELECT col2 FROM test ORDER BY col2"
        )?);
        Ok(())
    }
}
