//! Pluggable companion shadows for partitioned tables.
//!
//! A companion is an externally-provided table (typically a virtual table from
//! another extension, e.g. sqlite-vec's `vec0`) declared at
//! `CREATE VIRTUAL TABLE` time and kept in sync with the data partitions by the
//! extension. The core knows only the [`Companion`] trait; module-specific
//! behavior lives behind feature flags (see `vec`).

use std::fmt::Debug;
use std::ops::IndexMut;

use sqlite3_ext::params;
use sqlite3_ext::Connection;
use sqlite3_ext::FallibleIteratorMut;
use sqlite3_ext::FromValue;
use sqlite3_ext::Result as ExtResult;
use sqlite3_ext::ValueRef;

use crate::error::TableError;
use crate::ColumnDeclarations;

use super::shadow_tables::interface::PendingRow;

#[cfg(feature = "vec")]
mod vec;

/// A parsed `companion <name> USING <module>(<args>)` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompanionDecl {
    /// Companion name; the shadow table is `<base>_<name>`.
    pub name: String,
    /// Virtual table module providing the companion (e.g. `vec0`).
    pub module: String,
    /// Verbatim module arguments (e.g. `embedding float[384]`).
    pub args: String,
}

impl CompanionDecl {
    /// Parses a `companion <name> USING <module>(<args>)` clause.
    ///
    /// Returns `Ok(None)` when the argument is not a companion clause, so callers
    /// can treat non-companion arguments as regular column declarations.
    pub fn parse(arg: &str) -> Result<Option<Self>, TableError> {
        let tokens: Vec<&str> = arg.splitn(4, char::is_whitespace).collect();
        if tokens
            .first()
            .map(|token| token.to_lowercase())
            .as_deref()
            != Some("companion")
        {
            return Ok(None);
        }
        let invalid = || {
            TableError::ColumnDeclaration(format!(
                "Invalid companion option: {}. Expected format 'companion <name> USING <module>(<args>)'",
                arg
            ))
        };
        if tokens.len() != 4 || tokens[2].to_lowercase() != "using" {
            return Err(invalid());
        }
        let name = tokens[1].to_string();
        let rest = tokens[3].trim();
        let open = rest.find('(').ok_or_else(invalid)?;
        if !rest.ends_with(')') {
            return Err(invalid());
        }
        let module = rest[..open].trim().to_string();
        let args = rest[open + 1..rest.len() - 1].trim().to_string();
        if name.is_empty() || module.is_empty() || args.is_empty() {
            return Err(invalid());
        }
        Ok(Some(CompanionDecl { name, module, args }))
    }
}

/// A companion shadow table kept in sync with a partitioned table.
///
/// Implementations receive lifecycle callbacks from the extension's write paths.
/// All callbacks run on the same connection inside the caller's transaction, so
/// companion state stays consistent with the data partitions.
pub trait Companion: Debug {
    /// Companion name; the shadow table is `<base>_<name>`.
    fn name(&self) -> &str;

    /// Name of the companion's shadow table for the given base table.
    fn table_name(&self, base_name: &str) -> String {
        format!("{}_{}", base_name, self.name())
    }

    /// SQL creating the companion shadow table.
    fn create_sql(&self, base_name: &str) -> String;

    /// SQL dropping the companion shadow table.
    fn drop_sql(&self, base_name: &str) -> String {
        format!("DROP TABLE {}", self.table_name(base_name))
    }

    /// Called after a batch of rows has been flushed into a partition.
    /// `first_rowid` is the physical rowid of `rows[0]`; rowids are consecutive
    /// within the chunk.
    fn on_rows_flushed(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
        first_rowid: i64,
        rows: &[PendingRow],
    ) -> ExtResult<()>;

    /// Called after a single row has been inserted directly into a partition
    /// (used by partition-moving updates).
    fn on_row_inserted(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
        rowid: i64,
        values: &[&ValueRef],
    ) -> ExtResult<()>;

    /// Called after a row has been deleted (or replaced) in a partition.
    fn on_row_deleted(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
        rowid: i64,
    ) -> ExtResult<()>;

    /// Called when a partition is dropped (retention cleanup or table destroy).
    fn on_partition_dropped(
        &self,
        db: &Connection,
        base_name: &str,
        partition_value: i64,
    ) -> ExtResult<()>;
}

/// Extracts the indices of main-schema columns referenced by a companion's
/// module arguments. The first identifier of each comma-separated argument is
/// considered a potential column reference.
pub(crate) fn synced_column_indices(args: &str, columns: &ColumnDeclarations) -> Vec<usize> {
    args.split(',')
        .filter_map(|arg| arg.split_whitespace().next())
        .filter_map(|name| {
            columns
                .0
                .iter()
                .position(|column| column.get_name() == name)
        })
        .collect()
}

/// Instantiates a companion from its declaration. Unknown modules produce an
/// error pointing at the relevant Cargo feature.
pub fn instantiate(
    decl: &CompanionDecl,
    columns: &ColumnDeclarations,
) -> ExtResult<Box<dyn Companion>> {
    match decl.module.as_str() {
        #[cfg(feature = "vec")]
        "vec0" => Ok(Box::new(vec::VecCompanion::new(decl, columns)?)),
        #[cfg(test)]
        "mirror" => Ok(Box::new(tests::MirrorCompanion::new(decl, columns)?)),
        other => Err(sqlite3_ext::Error::Module(format!(
            "Unknown companion module '{}'. Available modules: {}",
            other,
            available_modules()
        ))),
    }
}

fn available_modules() -> String {
    #[cfg(feature = "vec")]
    {
        "vec0".to_string()
    }
    #[cfg(not(feature = "vec"))]
    {
        "(none; rebuild with --features vec for vec0 support)".to_string()
    }
}

// --- Companion declaration storage (`<base>_companions`) ---

const STORE_POSTFIX: &str = "companions";

fn store_name(base_name: &str) -> String {
    format!("{}_{}", base_name, STORE_POSTFIX)
}

fn store_exists(db: &Connection, base_name: &str) -> ExtResult<bool> {
    let count: i64 = db.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        params![store_name(base_name)],
        |row| Ok(row.index_mut(0).get_i64()),
    )?;
    Ok(count > 0)
}

pub fn create_store(db: &Connection, base_name: &str) -> ExtResult<()> {
    db.execute(
        &format!(
            "CREATE TABLE {} (name TEXT PRIMARY KEY, module TEXT, args TEXT)",
            store_name(base_name)
        ),
        (),
    )?;
    Ok(())
}

pub fn store_decl(db: &Connection, base_name: &str, decl: &CompanionDecl) -> ExtResult<()> {
    db.execute(
        &format!(
            "INSERT INTO {} (name, module, args) VALUES (?, ?, ?)",
            store_name(base_name)
        ),
        params![decl.name, decl.module, decl.args],
    )?;
    Ok(())
}

/// Loads companion declarations from storage. Returns an empty vec when the
/// table was created without companions.
pub fn load_decls(db: &Connection, base_name: &str) -> ExtResult<Vec<CompanionDecl>> {
    if !store_exists(db, base_name)? {
        return Ok(Vec::new());
    }
    let mut stmt = db.prepare(&format!(
        "SELECT name, module, args FROM {}",
        store_name(base_name)
    ))?;
    let mut decls = Vec::new();
    let rows = stmt.query(())?;
    while let Ok(Some(row)) = rows.next() {
        decls.push(CompanionDecl {
            name: row[0].get_str()?.to_owned(),
            module: row[1].get_str()?.to_owned(),
            args: row[2].get_str()?.to_owned(),
        });
    }
    Ok(decls)
}

pub fn drop_store(db: &Connection, base_name: &str) -> ExtResult<()> {
    if store_exists(db, base_name)? {
        db.execute(&format!("DROP TABLE {}", store_name(base_name)), ())?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::query::ToParam;
    use sqlite3_ext::Connection;
    use sqlite3_ext::FallibleIteratorMut;
    use sqlite3_ext::FromValue;
    use sqlite3_ext::Result as ExtResult;
    use sqlite3_ext::ValueRef;

    use crate::shadow_tables::interface::PendingRow;
    use crate::vtab_interface::init;
    use crate::ColumnDeclarations;

    use super::{synced_column_indices, Companion, CompanionDecl};

    /// Test-only companion mirroring synced columns into a plain table
    /// `<base>_mirror`, so the sync machinery can be tested without vec0.
    #[derive(Debug)]
    pub(crate) struct MirrorCompanion {
        name: String,
        sync_indices: Vec<usize>,
        sync_names: Vec<String>,
    }

    impl MirrorCompanion {
        pub(crate) fn new(decl: &CompanionDecl, columns: &ColumnDeclarations) -> ExtResult<Self> {
            let sync_indices = synced_column_indices(&decl.args, columns);
            if sync_indices.is_empty() {
                return Err(sqlite3_ext::Error::Module(
                    "mirror companion needs at least one synced column".into(),
                ));
            }
            Ok(MirrorCompanion {
                name: decl.name.clone(),
                sync_names: sync_indices
                    .iter()
                    .map(|&index| columns.0[index].get_name().to_string())
                    .collect(),
                sync_indices,
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

    impl Companion for MirrorCompanion {
        fn name(&self) -> &str {
            &self.name
        }

        fn create_sql(&self, base_name: &str) -> String {
            format!(
                "CREATE TABLE {} ({}, epoch integer, prowid integer)",
                self.table_name(base_name),
                self.sync_names.join(", ")
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
            let mut stmt = db.prepare(&self.insert_sql(base_name, rows.len()))?;
            let mut position = 1i32;
            for (row_offset, row) in rows.iter().enumerate() {
                for &index in &self.sync_indices {
                    row.values[index].clone().bind_param(&mut stmt, position)?;
                    position += 1;
                }
                partition_value.bind_param(&mut stmt, position)?;
                (first_rowid + row_offset as i64).bind_param(&mut stmt, position + 1)?;
                position += 2;
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
            let mut stmt = db.prepare(&self.insert_sql(base_name, 1))?;
            let mut position = 1i32;
            for &index in &self.sync_indices {
                values[index].bind_param(&mut stmt, position)?;
                position += 1;
            }
            partition_value.bind_param(&mut stmt, position)?;
            rowid.bind_param(&mut stmt, position + 1)?;
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

    use super::CompanionDecl as Decl;

    #[test]
    fn test_parse_companion_clause() {
        let decl = Decl::parse("companion vec USING vec0(embedding float[384])")
            .unwrap()
            .unwrap();
        assert_eq!(decl.name, "vec");
        assert_eq!(decl.module, "vec0");
        assert_eq!(decl.args, "embedding float[384]");
    }

    #[test]
    fn test_parse_non_companion_returns_none() {
        assert_eq!(Decl::parse("col1 text").unwrap(), None);
        assert_eq!(Decl::parse("lifetime 1 day").unwrap(), None);
    }

    #[test]
    fn test_parse_rejects_malformed() {
        assert!(Decl::parse("companion vec vec0(embedding float[4])").is_err());
        assert!(Decl::parse("companion vec USING vec0").is_err());
        assert!(Decl::parse("companion vec USING (embedding)").is_err());
    }

    fn mirror_rows(conn: &Connection) -> Vec<(String, i64, i64)> {
        let mut stmt = conn
            .prepare("SELECT col2, epoch, prowid FROM test_mirror ORDER BY epoch, prowid")
            .unwrap();
        let rows = stmt.query(()).unwrap();
        let mut out = Vec::new();
        while let Ok(Some(row)) = rows.next() {
            out.push((
                row[0].get_str().unwrap().to_owned(),
                row[1].get_i64(),
                row[2].get_i64(),
            ));
        }
        out
    }

    #[test]
    fn test_mirror_companion_syncs_flush_delete_update() -> ExtResult<()> {
        let rusq = RusqConn::open_in_memory().unwrap();
        let conn = Connection::from_rusqlite(&rusq);
        init(conn)?;
        conn.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text, companion mirror USING mirror(col2))",
            (),
        )?;

        // Inserts across two partitions, synced on flush.
        conn.execute(
            "INSERT INTO test VALUES ('2024-01-01 10:00', 'a'), ('2024-01-01 10:30', 'b'), ('2024-01-01 11:00', 'c')",
            (),
        )?;
        conn.query_row("SELECT count(*) FROM test", (), |row| Ok(row[0].get_i64()))?; // flush
        assert_eq!(
            mirror_rows(conn),
            vec![
                ("a".to_string(), 1704103200, 1),
                ("b".to_string(), 1704103200, 2),
                ("c".to_string(), 1704106800, 1),
            ]
        );

        // Non-moving update replaces the companion row.
        conn.execute(
            "UPDATE test SET col2 = 'b2' WHERE col1 = '2024-01-01 10:30' AND col2 = 'b'",
            (),
        )?;
        assert_eq!(
            mirror_rows(conn),
            vec![
                ("a".to_string(), 1704103200, 1),
                ("b2".to_string(), 1704103200, 2),
                ("c".to_string(), 1704106800, 1),
            ]
        );

        // Delete removes the companion row.
        conn.execute("DELETE FROM test WHERE col2 = 'b2'", ())?;
        assert_eq!(
            mirror_rows(conn),
            vec![("a".to_string(), 1704103200, 1), ("c".to_string(), 1704106800, 1)]
        );

        // Partition-moving update re-locates the companion row.
        conn.execute(
            "UPDATE test SET col1 = '2024-01-01 12:00' WHERE col2 = 'a'",
            (),
        )?;
        assert_eq!(
            mirror_rows(conn),
            vec![
                ("c".to_string(), 1704106800, 1),
                ("a".to_string(), 1704110400, 1),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_mirror_companion_purged_by_cleanup() -> ExtResult<()> {
        let rusq = RusqConn::open_in_memory().unwrap();
        let conn = Connection::from_rusqlite(&rusq);
        init(conn)?;
        conn.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, lifetime 1 day, col1 timestamp partition_column, col2 text, companion mirror USING mirror(col2))",
            (),
        )?;
        conn.execute(
            "INSERT INTO test VALUES ('2024-01-01 10:00', 'old'), ('2999-01-01 10:00', 'live')",
            (),
        )?;
        conn.query_row("SELECT count(*) FROM test", (), |row| Ok(row[0].get_i64()))?;
        assert_eq!(mirror_rows(conn).len(), 2);

        let dropped = crate::cleanup::cleanup_expired_partitions(conn, "test")?;
        assert_eq!(dropped, 1);
        assert_eq!(mirror_rows(conn).len(), 1);
        Ok(())
    }

    #[test]
    fn test_companion_decls_roundtrip_storage() -> ExtResult<()> {
        let rusq = RusqConn::open_in_memory().unwrap();
        let conn = Connection::from_rusqlite(&rusq);
        init(conn)?;
        conn.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text, companion mirror USING mirror(col2))",
            (),
        )?;
        let decls = super::load_decls(conn, "test")?;
        assert_eq!(
            decls,
            vec![Decl {
                name: "mirror".to_string(),
                module: "mirror".to_string(),
                args: "col2".to_string(),
            }]
        );
        Ok(())
    }

    #[test]
    fn test_unknown_companion_module_is_rejected() {
        let rusq = RusqConn::open_in_memory().unwrap();
        let conn = Connection::from_rusqlite(&rusq);
        init(conn).unwrap();
        let result = conn.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, companion nope USING nope(col1))",
            (),
        );
        assert!(result.is_err());
    }
}
