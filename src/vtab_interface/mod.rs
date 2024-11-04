pub mod operations;
mod vtab_cursor;
mod vtab_module;

use crate::constraints::{WhereClause, WhereClauses};
use crate::{shadow_tables::interface::VirtualTable, vtab_interface::vtab_module::*};
use operations::create::*;
use sqlite3_ext::{
    ffi::SQLITE_NOTFOUND,
    sqlite3_ext_main,
    vtab::{ChangeInfo, IndexInfoConstraint},
    Connection, Result as ExtResult,
};

use std::{collections::HashMap, sync::RwLock};

use crate::utils::parse_partition_value;

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
fn init(db: &Connection) -> ExtResult<()> {
    db.create_module(
        "Partitioner",
        PartitionMetaTable::module(),
        RwLock::default(),
    )?;
    Ok(())
}

/// Constructs `WhereClauses` from the provided index information and virtual table.
///
/// This function parses the index information to generate SQL WHERE clauses that are
/// applicable for querying the virtual table, based on its column constraints and indexes.
///
/// Parameters:
/// - `index_info`: Index information provided by the SQLite VTAB method bestIndex.
/// - `virtual_table`: Reference to the `VirtualTable`.
///
/// Returns:
/// - A result containing `WhereClauses` if successful, or an error on failure.
fn construct_where_clause(
    index_info: &sqlite3_ext::vtab::IndexInfo,
    virtual_table: &VirtualTable,
) -> ExtResult<WhereClauses> {
    let mut column_name_map: HashMap<String, Vec<(IndexInfoConstraint, i32)>> = HashMap::new();
    for (index, constraint) in index_info
        .constraints()
        .enumerate()
        .filter(|(_index, c)| c.usable())
    {
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

    use std::ops::{DerefMut, Index, IndexMut};

    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::{
        query::QueryResult, Connection, FallibleIterator, FallibleIteratorMut, FromValue,
    };

    use super::init;
    fn setup_db(rusq_conn: &RusqConn) -> &Connection {
        let conn = Connection::from_rusqlite(rusq_conn);
        conn
    }
    fn init_rusq_conn() -> RusqConn {
        RusqConn::open_in_memory().unwrap()
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
                    "CREATE TABLE test_root (partition_column TEXT, partition_value INTEGER, expiration_value INTEGER)"
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
                    "CREATE TABLE test_lookup (partition_table TEXT UNIQUE, partition_value INTEGER UNIQUE)"
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
}
