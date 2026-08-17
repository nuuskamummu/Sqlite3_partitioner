//! sqlite-vec (`vec0`) companion: a flat vector shadow table
//! `<base>_vec` whose rows carry `(embedding..., epoch, prowid)` locators back
//! to the data partition.
//!
//! The vec0 table is created with the user-supplied module arguments plus two
//! auxiliary columns: `epoch` (partition start) and `prowid` (physical rowid
//! within the partition). Sync happens on flush/insert/delete, and retention
//! purges by epoch.

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

    fn insert_sql(&self, base_name: &str, row_count: usize) -> String {
        let columns = self
            .sync_names
            .iter()
            .cloned()
            .chain(["epoch".to_string(), "prowid".to_string()])
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholders = std::iter::repeat("?")
            .take(self.sync_names.len() + 2)
            .collect::<Vec<_>>()
            .join(",");
        let values_clause = (0..row_count)
            .map(|_| format!("({})", row_placeholders))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.table_name(base_name),
            columns,
            values_clause
        )
    }
}

impl Companion for VecCompanion {
    fn name(&self) -> &str {
        &self.name
    }

    fn create_sql(&self, base_name: &str) -> String {
        format!(
            "CREATE VIRTUAL TABLE {} USING vec0({}, +epoch integer, +prowid integer)",
            self.table_name(base_name),
            self.args
        )
    }

    fn on_rows_flushed(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
        first_rowid: i64,
        rows: &[PendingRow],
    ) -> ExtResult<()> {
        let sql = self.insert_sql(base_name, rows.len());
        let mut stmt = db.prepare(&sql)?;
        let mut position = 1i32;
        for (row_offset, row) in rows.iter().enumerate() {
            for &index in &self.sync_indices {
                row.values[index].clone().bind_param(&mut stmt, position)?;
                position += 1;
            }
            position = bind_locator(&mut stmt, position, partition_value, first_rowid, row_offset)?;
        }
        stmt.execute(())?;
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
        let sql = self.insert_sql(base_name, 1);
        let mut stmt = db.prepare(&sql)?;
        let mut position = 1i32;
        for &index in &self.sync_indices {
            values[index].bind_param(&mut stmt, position)?;
            position += 1;
        }
        bind_locator(&mut stmt, position, partition_value, rowid, 0)?;
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
                "DELETE FROM {} WHERE epoch = ? AND prowid = ?",
                self.table_name(base_name)
            ),
            sqlite3_ext::params![partition_value, rowid],
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
                "DELETE FROM {} WHERE epoch = ?",
                self.table_name(base_name)
            ),
            sqlite3_ext::params![partition_value],
        )?;
        Ok(())
    }
}

fn bind_locator(
    stmt: &mut sqlite3_ext::query::Statement,
    position: i32,
    partition_value: i64,
    first_rowid: i64,
    row_offset: usize,
) -> ExtResult<i32> {
    partition_value.bind_param(stmt, position)?;
    (first_rowid + row_offset as i64).bind_param(stmt, position + 1)?;
    Ok(position + 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnDeclaration;
    use crate::ColumnDeclarations;
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
    fn test_create_sql_adds_locator_columns() {
        let companion = VecCompanion::new(&decl("embedding float[4]"), &columns()).unwrap();
        assert_eq!(
            companion.create_sql("events"),
            "CREATE VIRTUAL TABLE events_vec USING vec0(embedding float[4], +epoch integer, +prowid integer)"
        );
    }

    #[test]
    fn test_insert_sql_binds_synced_columns_and_locator() {
        let companion = VecCompanion::new(&decl("embedding float[4]"), &columns()).unwrap();
        assert_eq!(
            companion.insert_sql("events", 2),
            "INSERT INTO events_vec (embedding, epoch, prowid) VALUES (?,?,?),(?,?,?)"
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
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, emb text, companion vec USING vec0(emb float[4]))",
            (),
        )
        .unwrap();

        conn.execute(
            "INSERT INTO test VALUES
             ('2024-01-01 10:00', '[1.0, 0.0, 0.0, 0.0]'),
             ('2024-01-01 10:30', '[0.9, 0.1, 0.0, 0.0]'),
             ('2024-01-01 11:00', '[0.0, 0.0, 1.0, 0.0]')",
            (),
        )
        .unwrap();
        conn.query_row("SELECT count(*) FROM test", (), |row| Ok(row[0].get_i64()))
            .unwrap(); // flush

        // KNN over the flat vector shadow.
        let hits: Vec<(i64, i64)> = {
            let mut stmt = conn
                .prepare(
                    "SELECT epoch, prowid FROM test_vec
                     WHERE emb MATCH '[1.0, 0.0, 0.0, 0.0]' AND k = 2 ORDER BY distance",
                )
                .unwrap();
            let rows = stmt.query(()).unwrap();
            let mut out = Vec::new();
            while let Ok(Some(row)) = rows.next() {
                out.push((row[0].get_i64(), row[1].get_i64()));
            }
            out
        };
        assert_eq!(hits, vec![(1704103200, 1), (1704103200, 2)]);

        // Resolve the top hit back to the actual row via the locator.
        let (epoch, prowid) = hits[0];
        let device: String = conn
            .query_row(
                &format!("SELECT col1 FROM test_{} WHERE rowid = ?", epoch),
                [prowid],
                |row| Ok(row[0].get_str()?.to_owned()),
            )
            .unwrap();
        assert_eq!(device, "2024-01-01 10:00");

        // Retention purge removes the vectors of expired partitions.
        let mut stmt = conn
            .prepare("DELETE FROM test_vec WHERE epoch = ?")
            .unwrap();
        stmt.execute([1704103200i64]).unwrap();
        let remaining: i64 = conn
            .query_row("SELECT count(*) FROM test_vec", (), |row| Ok(row[0].get_i64()))
            .unwrap();
        assert_eq!(remaining, 1);
    }
}
