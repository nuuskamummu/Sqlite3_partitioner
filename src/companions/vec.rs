//! sqlite-vec (`vec0`) companion: one vector table per data partition.
//!
//! A data partition `<base>_<epoch>` has a paired vec0 table
//! `<base>_<epoch>_<companion>`. Both tables use the same rowids, so vector
//! hits can be resolved directly inside the matching data partition. Dropping a
//! partition drops its vector index too; no global vector table or locator index
//! is maintained.

use sqlite3_ext::query::ToParam;
use sqlite3_ext::Connection;
use sqlite3_ext::Result as ExtResult;
use sqlite3_ext::ValueRef;

use crate::shadow_tables::interface::PendingRow;
use crate::ColumnDeclarations;

use super::{synced_column_indices, Companion, CompanionDecl};

#[derive(Debug)]
pub struct VecCompanion {
    name: String,
    args: String,
    /// Indices (in declared column order) of the main columns synced to vec0.
    sync_indices: Vec<usize>,
    /// Names of the synced columns, used in INSERT statements.
    sync_names: Vec<String>,
}

impl VecCompanion {
    pub fn new(decl: &CompanionDecl, columns: &ColumnDeclarations) -> ExtResult<Self> {
        let sync_indices = synced_column_indices(&decl.args, columns);
        if sync_indices.is_empty() {
            return Err(sqlite3_ext::Error::Module(format!(
                "companion '{}': none of the vec0 arguments reference a column of the table",
                decl.name
            )));
        }
        let sync_names = sync_indices
            .iter()
            .map(|&index| columns.0[index].get_name().to_string())
            .collect();
        Ok(VecCompanion {
            name: decl.name.clone(),
            args: decl.args.clone(),
            sync_indices,
            sync_names,
        })
    }

    fn partition_table_name(&self, base_name: &str, partition_value: i64) -> String {
        format!("{}_{}_{}", base_name, partition_value, self.name)
    }

    fn insert_sql(&self, base_name: &str, partition_value: i64, row_count: usize) -> String {
        let columns = std::iter::once("rowid".to_string())
            .chain(self.sync_names.iter().cloned())
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholders = std::iter::repeat("?")
            .take(self.sync_names.len() + 1)
            .collect::<Vec<_>>()
            .join(",");
        let values_clause = (0..row_count)
            .map(|_| format!("({})", row_placeholders))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.partition_table_name(base_name, partition_value),
            columns,
            values_clause
        )
    }
}

impl Companion for VecCompanion {
    fn name(&self) -> &str {
        &self.name
    }

    fn on_partition_created(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
    ) -> ExtResult<()> {
        db.execute(
            &format!(
                "CREATE VIRTUAL TABLE IF NOT EXISTS {} USING vec0({})",
                self.partition_table_name(base_name, partition_value),
                self.args
            ),
            (),
        )?;
        Ok(())
    }

    fn on_rows_flushed(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
        first_rowid: i64,
        rows: &[PendingRow],
    ) -> ExtResult<()> {
        let mut stmt = db.prepare(&self.insert_sql(base_name, partition_value, rows.len()))?;
        let mut position = 1i32;
        for (row_offset, row) in rows.iter().enumerate() {
            (first_rowid + row_offset as i64).bind_param(&mut stmt, position)?;
            position += 1;
            for &index in &self.sync_indices {
                row.values[index].clone().bind_param(&mut stmt, position)?;
                position += 1;
            }
        }
        stmt.execute(()).map_err(|err| {
            sqlite3_ext::Error::Module(format!(
                "vec companion batch insert into {} failed: {}",
                self.partition_table_name(base_name, partition_value),
                err
            ))
        })?;
        Ok(())
    }

    fn on_row_inserted(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
        rowid: i64,
        values: &[&ValueRef],
    ) -> ExtResult<()> {
        let mut stmt = db.prepare(&self.insert_sql(base_name, partition_value, 1))?;
        rowid.bind_param(&mut stmt, 1)?;
        for (position, &index) in self.sync_indices.iter().enumerate() {
            values[index].bind_param(&mut stmt, position as i32 + 2)?;
        }
        stmt.execute(())?;
        Ok(())
    }

    fn on_row_deleted(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
        rowid: i64,
    ) -> ExtResult<()> {
        db.execute(
            &format!(
                "DELETE FROM {} WHERE rowid = ?",
                self.partition_table_name(base_name, partition_value)
            ),
            sqlite3_ext::params![rowid],
        )?;
        Ok(())
    }

    fn on_partition_dropped(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
    ) -> ExtResult<()> {
        db.execute(
            &format!(
                "DROP TABLE IF EXISTS {}",
                self.partition_table_name(base_name, partition_value)
            ),
            (),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnDeclaration;
    use sqlite3_ext::{FallibleIteratorMut, FromValue};

    fn decl(args: &str) -> CompanionDecl {
        CompanionDecl {
            name: "vec".to_string(),
            module: "vec0".to_string(),
            args: args.to_string(),
        }
    }

    fn columns() -> ColumnDeclarations {
        ColumnDeclarations(vec![
            ColumnDeclaration::try_from("ts timestamp partition_column").unwrap(),
            ColumnDeclaration::try_from("device_id text").unwrap(),
            ColumnDeclaration::try_from("embedding text").unwrap(),
        ])
    }

    #[test]
    fn test_partition_table_name() {
        let companion = VecCompanion::new(&decl("embedding float[4]"), &columns()).unwrap();
        assert_eq!(
            companion.partition_table_name("events", 1704103200),
            "events_1704103200_vec"
        );
    }

    #[test]
    fn test_insert_sql_binds_data_rowids() {
        let companion = VecCompanion::new(&decl("embedding float[4]"), &columns()).unwrap();
        assert_eq!(
            companion.insert_sql("events", 1704103200, 2),
            "INSERT INTO events_1704103200_vec (rowid, embedding) VALUES (?,?),(?,?)"
        );
    }

    #[test]
    fn test_requires_at_least_one_synced_column() {
        assert!(VecCompanion::new(&decl("nope float[4]"), &columns()).is_err());
    }

    /// End-to-end against a real vec0 extension. Skipped unless
    /// VEC0_EXTENSION_PATH points at a loadable vec0 library.
    #[test]
    fn test_vec0_end_to_end() {
        let vec0_path = match std::env::var("VEC0_EXTENSION_PATH") {
            Ok(path) => path,
            Err(_) => {
                eprintln!("skipping: VEC0_EXTENSION_PATH not set");
                return;
            }
        };
        let rusq = rusqlite::Connection::open_in_memory().unwrap();
        unsafe {
            rusq.load_extension_enable().unwrap();
            rusq.load_extension(&vec0_path, None).unwrap();
            rusq.load_extension_disable().unwrap();
        }
        let conn = sqlite3_ext::Connection::from_rusqlite(&rusq);
        crate::vtab_interface::init(conn).unwrap();
        conn.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, lifetime 1 day, col1 timestamp partition_column, emb text, companion vec USING vec0(emb float[4]))",
            (),
        )
        .unwrap();

        conn.execute(
            "INSERT INTO test VALUES
             ('2024-01-01 10:00', '[1.0, 0.0, 0.0, 0.0]'),
             ('2024-01-01 10:30', '[0.9, 0.1, 0.0, 0.0]'),
             ('2999-01-01 11:00', '[0.0, 0.0, 1.0, 0.0]')",
            (),
        )
        .unwrap();
        conn.query_row("SELECT count(*) FROM test", (), |row| Ok(row[0].get_i64()))
            .unwrap(); // flush

        let hits: Vec<i64> = {
            let mut stmt = conn
                .prepare(
                    "SELECT rowid FROM test_1704103200_vec
                     WHERE emb MATCH '[1.0, 0.0, 0.0, 0.0]' AND k = 2 ORDER BY distance",
                )
                .unwrap();
            let rows = stmt.query(()).unwrap();
            let mut out = Vec::new();
            while let Ok(Some(row)) = rows.next() {
                out.push(row[0].get_i64());
            }
            out
        };
        assert_eq!(hits, vec![1, 2]);

        let timestamp: String = conn
            .query_row(
                "SELECT col1 FROM test_1704103200 WHERE rowid = ?",
                [hits[0]],
                |row| Ok(row[0].get_str()?.to_owned()),
            )
            .unwrap();
        assert_eq!(timestamp, "2024-01-01 10:00");

        let dropped = crate::cleanup::cleanup_expired_partitions(conn, "test").unwrap();
        assert_eq!(dropped, 1);
        let old_vec_exists: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'test_1704103200_vec'",
                (),
                |row| Ok(row[0].get_i64()),
            )
            .unwrap();
        assert_eq!(old_vec_exists, 0);
        let remaining: i64 = conn
            .query_row("SELECT count(*) FROM test_32472183600_vec", (), |row| {
                Ok(row[0].get_i64())
            })
            .unwrap();
        assert_eq!(remaining, 1);
    }

    /// Dropping a partitioned table with a vec companion must drop the
    /// per-partition vec0 tables too (destroy path).
    #[test]
    fn test_vec0_destroy_drops_companion_tables() {
        let vec0_path = match std::env::var("VEC0_EXTENSION_PATH") {
            Ok(path) => path,
            Err(_) => {
                eprintln!("skipping: VEC0_EXTENSION_PATH not set");
                return;
            }
        };
        let rusq = rusqlite::Connection::open_in_memory().unwrap();
        unsafe {
            rusq.load_extension_enable().unwrap();
            rusq.load_extension(&vec0_path, None).unwrap();
            rusq.load_extension_disable().unwrap();
        }
        let conn = sqlite3_ext::Connection::from_rusqlite(&rusq);
        crate::vtab_interface::init(conn).unwrap();
        conn.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, emb text, companion vec USING vec0(emb float[4]))",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO test VALUES ('2024-01-01 10:00', '[1.0, 0.0, 0.0, 0.0]'), ('2024-01-01 11:00', '[0.9, 0.1, 0.0, 0.0]')",
            (),
        )
        .unwrap();
        conn.query_row("SELECT count(*) FROM test", (), |row| Ok(row[0].get_i64()))
            .unwrap(); // flush

        // Mimic the benchmark's manual retention purge: drop one partition
        // pair by hand and remove its lookup/stats rows before the destroy.
        conn.execute("DROP TABLE test_1704103200_vec", ()).unwrap();
        conn.execute("DROP TABLE test_1704103200", ()).unwrap();
        conn.execute(
            "DELETE FROM test_lookup WHERE partition_value = 1704103200",
            (),
        )
        .unwrap();
        conn.execute(
            "DELETE FROM test_stats WHERE partition_table = 'test_1704103200'",
            (),
        )
        .unwrap();

        conn.execute("DROP TABLE test", ())
            .expect("DROP TABLE with vec companion failed");

        let leftovers: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name LIKE 'test%'",
                (),
                |row| Ok(row[0].get_i64()),
            )
            .unwrap();
        assert_eq!(leftovers, 0);
    }
}
