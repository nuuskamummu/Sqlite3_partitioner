//! sqlite-vec (`vec0`) companion: one vector table per data partition.
//!
//! A data partition `<base>_<epoch>` has a paired vec0 table
//! `<base>_<epoch>_<companion>`. Both tables use the same rowids, so vector
//! hits can be resolved directly inside the matching data partition. Dropping a
//! partition drops its vector index too; no global vector table or locator index
//! is maintained.

use sqlite3_ext::query::ToParam;
use sqlite3_ext::vtab::ConstraintOp;
use sqlite3_ext::Connection;
use sqlite3_ext::FallibleIteratorMut;
use sqlite3_ext::FromValue;
use sqlite3_ext::Result as ExtResult;
use sqlite3_ext::Value;
use sqlite3_ext::ValueRef;

use crate::shadow_tables::interface::PendingRow;
use crate::ColumnDeclaration;
use crate::ColumnDeclarations;

use super::{synced_column_indices, Companion, CompanionDecl, CompanionHit};

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

    fn hidden_columns(&self) -> Vec<ColumnDeclaration> {
        let mut k = ColumnDeclaration::new("k".into(), sqlite3_ext::ValueType::Integer);
        k.set_hidden();
        let mut distance = ColumnDeclaration::new("distance".into(), sqlite3_ext::ValueType::Float);
        distance.set_hidden();
        vec![k, distance]
    }

    fn drives_scan(&self, column: &str, op: ConstraintOp) -> bool {
        matches!(op, ConstraintOp::Match) && self.sync_names.iter().any(|name| name == column)
    }

    fn scan_param(&self, hidden_column: &str, op: ConstraintOp) -> bool {
        matches!(op, ConstraintOp::Eq) && hidden_column == "k"
    }

    fn orders_scan_by(&self, hidden_column: &str) -> bool {
        hidden_column == "distance"
    }

    /// Per-partition KNN merged into a global ascending-distance top-k.
    ///
    /// The merge is exact: the global top-k is a subset of the union of the
    /// per-partition top-k sets, so re-ranking those candidates suffices.
    fn scan(
        &self,
        db: &Connection,
        _base_name: &str,
        partitions: &[(i64, String)],
        driver: &ValueRef,
        params: &[Option<&ValueRef>],
    ) -> ExtResult<Vec<CompanionHit>> {
        let k = params
            .first()
            .and_then(|value| *value)
            .ok_or_else(|| {
                sqlite3_ext::Error::Module(
                    "MATCH scans on this table require a `k = <n>` constraint".to_string(),
                )
            })?
            .get_i64();
        if k < 1 {
            return Err(sqlite3_ext::Error::Module(
                "`k` must be a positive integer".to_string(),
            ));
        }
        let embedding_column = self.sync_names.first().ok_or_else(|| {
            sqlite3_ext::Error::Module("vec companion has no synced column".to_string())
        })?;

        let mut hits: Vec<(f64, i64, i64)> = Vec::new(); // (distance, partition_value, rowid)
        for (partition_value, partition_name) in partitions {
            let vec_table = format!("{}_{}", partition_name, self.name);
            let mut stmt = db
                .prepare(&format!(
                    "SELECT rowid, distance FROM {} WHERE {} MATCH ? AND k = ?",
                    vec_table, embedding_column
                ))
                .map_err(|err| {
                    sqlite3_ext::Error::Module(format!("vec scan prepare {}: {}", vec_table, err))
                })?;
            driver.bind_param(&mut stmt, 1).map_err(|err| {
                sqlite3_ext::Error::Module(format!("vec scan bind driver: {}", err))
            })?;
            k.bind_param(&mut stmt, 2)
                .map_err(|err| sqlite3_ext::Error::Module(format!("vec scan bind k: {}", err)))?;
            let rows = stmt.query(()).map_err(|err| {
                sqlite3_ext::Error::Module(format!("vec scan query {}: {}", vec_table, err))
            })?;
            let mut collected = Vec::new();
            loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        collected.push((row[1].get_f64(), *partition_value, row[0].get_i64()))
                    }
                    Ok(None) => break,
                    Err(err) => {
                        return Err(sqlite3_ext::Error::Module(format!(
                            "vec scan step {}: {}",
                            vec_table, err
                        )))
                    }
                }
            }
            hits.extend(collected);
        }
        hits.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        hits.truncate(k as usize);

        Ok(hits
            .into_iter()
            .map(|(distance, partition_value, prowid)| CompanionHit {
                partition_value,
                prowid,
                // Hidden columns in declaration order: k (constraint-only -> NULL),
                // distance (the per-row value).
                hidden: vec![Value::Null, Value::Float(distance)],
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnDeclaration;
    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::{FallibleIteratorMut, FromValue};

    /// Opens an in-memory connection with vec0 and the partitioner loaded.
    /// Returns None (test should skip) when VEC0_EXTENSION_PATH is unset.
    fn vec0_test_conn() -> Option<RusqConn> {
        let vec0_path = std::env::var("VEC0_EXTENSION_PATH").ok()?;
        let rusq = RusqConn::open_in_memory().unwrap();
        unsafe {
            rusq.load_extension_enable().unwrap();
            rusq.load_extension(&vec0_path, None).unwrap();
            rusq.load_extension_disable().unwrap();
        }
        let conn = sqlite3_ext::Connection::from_rusqlite(&rusq);
        crate::vtab_interface::init(conn).unwrap();
        conn.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, emb text, col2 text, companion vec USING vec0(emb float[4]))",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO test VALUES
             ('2024-01-01 10:00', '[1.0, 0.0, 0.0, 0.0]', 'a'),
             ('2024-01-01 10:30', '[0.9, 0.1, 0.0, 0.0]', 'b'),
             ('2024-01-01 11:00', '[0.0, 0.0, 1.0, 0.0]', 'c')",
            (),
        )
        .unwrap();
        Some(rusq)
    }

    fn knn_rows(conn: &Connection, sql: &str) -> Vec<(String, f64)> {
        let mut stmt = conn.prepare(sql).unwrap();
        let rows = stmt.query(()).unwrap();
        let mut out = Vec::new();
        loop {
            match rows.next() {
                Ok(Some(row)) => {
                    out.push((row[0].get_str().unwrap().to_owned(), row[1].get_f64()))
                }
                Ok(None) => break,
                Err(err) => panic!("knn query failed: {:?}", err),
            }
        }
        out
    }

    /// Pure-SQL KNN on the partitioned vtab: per-partition top-k merged and
    /// re-ranked globally, consumed ORDER BY distance.
    #[test]
    fn test_vec0_sql_knn_merges_partitions() {
        let Some(rusq) = vec0_test_conn() else {
            eprintln!("skipping: VEC0_EXTENSION_PATH not set");
            return;
        };
        let conn = sqlite3_ext::Connection::from_rusqlite(&rusq);

        let rows = knn_rows(
            conn,
            "SELECT col1, distance FROM test
             WHERE emb MATCH '[1.0, 0.0, 0.0, 0.0]' AND k = 3
             ORDER BY distance",
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, "2024-01-01 10:00");
        assert_eq!(rows[0].1, 0.0);
        assert_eq!(rows[1].0, "2024-01-01 10:30");
        assert_eq!(rows[2].0, "2024-01-01 11:00");
        assert!(rows[0].1 <= rows[1].1 && rows[1].1 <= rows[2].1);

        // k truncates the merged result.
        let rows = knn_rows(
            conn,
            "SELECT col1, distance FROM test
             WHERE emb MATCH '[1.0, 0.0, 0.0, 0.0]' AND k = 1
             ORDER BY distance",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "2024-01-01 10:00");
    }

    /// MATCH combined with partition-column predicates prunes partitions
    /// before the companion scan runs.
    #[test]
    fn test_vec0_sql_knn_with_time_window() {
        let Some(rusq) = vec0_test_conn() else {
            eprintln!("skipping: VEC0_EXTENSION_PATH not set");
            return;
        };
        let conn = sqlite3_ext::Connection::from_rusqlite(&rusq);

        let rows = knn_rows(
            conn,
            "SELECT col1, distance FROM test
             WHERE emb MATCH '[1.0, 0.0, 0.0, 0.0]' AND k = 3
               AND col1 >= '2024-01-01 10:15' AND col1 < '2024-01-01 11:00'
             ORDER BY distance",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "2024-01-01 10:30");

        let rows = knn_rows(
            conn,
            "SELECT col1, distance FROM test
             WHERE emb MATCH '[0.0, 0.0, 1.0, 0.0]' AND k = 3
               AND col1 < '2024-01-01 11:00'
             ORDER BY distance",
        );
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|(ts, _)| ts != "2024-01-01 11:00"));
    }

    /// Rows produced by a MATCH scan carry working rowids: they can be
    /// updated and deleted through the vtab, and the companion stays in sync.
    #[test]
    fn test_vec0_sql_knn_rowids_support_update_delete() {
        let Some(rusq) = vec0_test_conn() else {
            eprintln!("skipping: VEC0_EXTENSION_PATH not set");
            return;
        };
        let conn = sqlite3_ext::Connection::from_rusqlite(&rusq);

        let nearest: i64 = conn
            .query_row(
                "SELECT rowid FROM test WHERE emb MATCH '[1.0, 0.0, 0.0, 0.0]' AND k = 1",
                (),
                |row| Ok(row[0].get_i64()),
            )
            .unwrap();

        conn.execute("UPDATE test SET col2 = 'nearest' WHERE rowid = ?", [nearest])
            .unwrap();
        let col2: String = conn
            .query_row("SELECT col2 FROM test WHERE rowid = ?", [nearest], |row| {
                Ok(row[0].get_str()?.to_owned())
            })
            .unwrap();
        assert_eq!(col2, "nearest");

        conn.execute("DELETE FROM test WHERE rowid = ?", [nearest])
            .unwrap();
        let remaining: i64 = conn
            .query_row("SELECT count(*) FROM test", (), |row| Ok(row[0].get_i64()))
            .unwrap();
        assert_eq!(remaining, 2);
        // The companion row is gone too: next nearest is now the 10:30 row.
        let rows = knn_rows(
            conn,
            "SELECT col1, distance FROM test
             WHERE emb MATCH '[1.0, 0.0, 0.0, 0.0]' AND k = 3
             ORDER BY distance",
        );
        assert_eq!(rows[0].0, "2024-01-01 10:30");
    }

    /// MATCH scans require a `k` constraint; MATCH on a table without a
    /// scan-driving companion is rejected at plan time.
    #[test]
    fn test_vec0_sql_match_error_cases() {
        let Some(rusq) = vec0_test_conn() else {
            eprintln!("skipping: VEC0_EXTENSION_PATH not set");
            return;
        };
        let conn = sqlite3_ext::Connection::from_rusqlite(&rusq);

        let result = conn.query_row(
            "SELECT col1 FROM test WHERE emb MATCH '[1.0, 0.0, 0.0, 0.0]'",
            (),
            |row| Ok(row[0].get_str()?.to_owned()),
        );
        assert!(result.is_err());

        conn.execute(
            "CREATE VIRTUAL TABLE plain USING partitioner(1 hour, col1 timestamp partition_column, col2 text)",
            (),
        )
        .unwrap();
        assert!(conn
            .prepare("SELECT col1 FROM plain WHERE col2 MATCH 'x'")
            .is_err());
    }

    /// Hidden columns must not leak into physical writes: inserts listing only
    /// real columns and plain updates keep working with hidden columns present.
    #[test]
    fn test_vec0_writes_ignore_hidden_columns() {
        let Some(rusq) = vec0_test_conn() else {
            eprintln!("skipping: VEC0_EXTENSION_PATH not set");
            return;
        };
        let conn = sqlite3_ext::Connection::from_rusqlite(&rusq);

        conn.execute(
            "INSERT INTO test (col1, emb, col2) VALUES ('2024-01-01 12:00', '[0.5, 0.5, 0.0, 0.0]', 'd')",
            (),
        )
        .unwrap();
        conn.execute(
            "UPDATE test SET col2 = 'b2' WHERE col1 = '2024-01-01 10:30'",
            (),
        )
        .unwrap();
        let count: i64 = conn
            .query_row("SELECT count(*) FROM test", (), |row| Ok(row[0].get_i64()))
            .unwrap();
        assert_eq!(count, 4);
        let updated: String = conn
            .query_row(
                "SELECT col2 FROM test WHERE col1 = '2024-01-01 10:30'",
                (),
                |row| Ok(row[0].get_str()?.to_owned()),
            )
            .unwrap();
        assert_eq!(updated, "b2");
    }

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
