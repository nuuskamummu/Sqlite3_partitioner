use sqlite3_ext::query::{Statement, ToParam};
use sqlite3_ext::{Connection, FallibleIteratorMut, FromValue, Result as ExtResult, ValueType};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::ColumnDeclaration;
use crate::ColumnDeclarations;

use super::operations::{Connect, Create, Drop, SchemaDeclaration, Table};

#[derive(Debug, Clone, Copy, Default)]
pub struct PartitionStats {
    pub row_count: i64,
}

#[derive(Debug)]
pub struct StatsTable {
    pub(super) schema: SchemaDeclaration,
    stats: RwLock<HashMap<String, PartitionStats>>,
    row_count_update_statement: RefCell<Option<Statement>>,
}

impl StatsTable {
    const PARTITION_TABLE_COLUMN: &'static str = "partition_table";
    const ROW_COUNT_COLUMN: &'static str = "row_count";
    const COLUMNS: &'static [ColumnDeclaration] = &[
        ColumnDeclaration::new(
            std::borrow::Cow::Borrowed(Self::PARTITION_TABLE_COLUMN),
            ValueType::Text,
        ),
        ColumnDeclaration::new(
            std::borrow::Cow::Borrowed(Self::ROW_COUNT_COLUMN),
            ValueType::Integer,
        ),
    ];

    pub fn partition_table_column(&self) -> &'static ColumnDeclaration {
        &Self::COLUMNS[0]
    }

    pub fn row_count_column(&self) -> &'static ColumnDeclaration {
        &Self::COLUMNS[1]
    }

    pub fn create(db: &Connection, base_name: &str) -> ExtResult<Self> {
        let table_name = Self::format_name(base_name);
        let schema =
            <Self as Create>::schema(db, table_name, ColumnDeclarations(Self::COLUMNS.to_vec()))?;
        Ok(Self {
            schema,
            stats: RwLock::default(),
            row_count_update_statement: RefCell::new(None),
        })
    }

    pub fn connect(db: &Connection, base_name: &str) -> ExtResult<Self> {
        let table_name = &Self::format_name(base_name);
        let schema = <Self as Connect>::schema(db, table_name)?;
        let table = Self {
            schema,
            stats: RwLock::new(HashMap::new()),
            row_count_update_statement: RefCell::new(None),
        };
        table.sync(db)?;
        Ok(table)
    }

    pub fn sync(&self, db: &Connection) -> ExtResult<()> {
        let sql = format!(
            "SELECT {}, {} FROM {};",
            self.partition_table_column().get_name(),
            self.row_count_column().get_name(),
            self.name(),
        );
        let mut statement = db.prepare(&sql)?;
        let rows = statement.query(())?;
        let mut borrowed_stats = self.stats.write().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error acquiring write lock on stats: {}", err)),
            )
        })?;
        borrowed_stats.clear();
        while let Ok(Some(row)) = rows.next() {
            borrowed_stats.insert(
                row[0].get_str()?.to_string(),
                PartitionStats {
                    row_count: row[1].get_i64(),
                },
            );
        }
        Ok(())
    }

    fn insert_query(&self) -> String {
        format!(
            "INSERT INTO {} ({}, {}) VALUES (?, ?)",
            self.name(),
            self.partition_table_column().get_name(),
            self.row_count_column().get_name(),
        )
    }

    pub fn insert_partition(&self, db: &Connection, partition_name: &str) -> ExtResult<()> {
        Connection::prepare(db, &self.insert_query())?.execute(|stmt: &mut Statement| {
            partition_name.bind_param(stmt, 1)?;
            0i64.bind_param(stmt, 2)?;
            Ok(())
        })?;
        let mut borrowed_stats = self.stats.write().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error acquiring write lock on stats: {}", err)),
            )
        })?;
        borrowed_stats.insert(partition_name.to_string(), PartitionStats::default());
        Ok(())
    }

    pub fn delete_partition(&self, db: &Connection, partition_name: &str) -> ExtResult<()> {
        let sql = format!(
            "DELETE FROM {} WHERE {} = ?;",
            self.name(),
            self.partition_table_column().get_name(),
        );
        db.execute(&sql, [partition_name])?;
        let mut borrowed_stats = self.stats.write().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error acquiring write lock on stats: {}", err)),
            )
        })?;
        borrowed_stats.remove(partition_name);
        Ok(())
    }

    fn update_row_count(&self, db: &Connection, partition_name: &str, delta: i64) -> ExtResult<()> {
        let mut cached_statement = self.row_count_update_statement.borrow_mut();
        if cached_statement.is_none() {
            let sql = format!(
                "UPDATE {} SET {} = {} + ? WHERE {} = ?;",
                self.name(),
                self.row_count_column().get_name(),
                self.row_count_column().get_name(),
                self.partition_table_column().get_name(),
            );
            *cached_statement = Some(Connection::prepare(db, &sql)?);
        }
        cached_statement
            .as_mut()
            .expect("row count update statement should be initialized")
            .execute(|stmt: &mut Statement| {
                delta.bind_param(stmt, 1)?;
                partition_name.bind_param(stmt, 2)?;
                Ok(())
            })?;
        drop(cached_statement);
        let mut borrowed_stats = self.stats.write().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error acquiring write lock on stats: {}", err)),
            )
        })?;
        let entry = borrowed_stats
            .entry(partition_name.to_string())
            .or_insert_with(PartitionStats::default);
        entry.row_count = (entry.row_count + delta).max(0);
        Ok(())
    }

    pub fn increment_row_count(&self, db: &Connection, partition_name: &str) -> ExtResult<()> {
        self.update_row_count(db, partition_name, 1)
    }

    pub fn decrement_row_count(&self, db: &Connection, partition_name: &str) -> ExtResult<()> {
        self.update_row_count(db, partition_name, -1)
    }

    pub fn increment_row_count_by(
        &self,
        db: &Connection,
        partition_name: &str,
        count: i64,
    ) -> ExtResult<()> {
        self.update_row_count(db, partition_name, count)
    }

    pub fn row_count(&self, partition_name: &str) -> ExtResult<Option<i64>> {
        let borrowed_stats = self.stats.read().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error acquiring read lock on stats: {}", err)),
            )
        })?;
        Ok(borrowed_stats
            .get(partition_name)
            .map(|stats| stats.row_count))
    }

    pub fn total_rows(&self) -> ExtResult<i64> {
        let borrowed_stats = self.stats.read().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error acquiring read lock on stats: {}", err)),
            )
        })?;
        Ok(borrowed_stats.values().map(|stats| stats.row_count).sum())
    }

    pub fn partition_count(&self) -> ExtResult<usize> {
        let borrowed_stats = self.stats.read().map_err(|err| {
            sqlite3_ext::Error::Sqlite(
                1,
                Some(format!("Error acquiring read lock on stats: {}", err)),
            )
        })?;
        Ok(borrowed_stats.len())
    }
}

impl Table for StatsTable {
    const POSTFIX: &'static str = "stats";

    fn schema(&self) -> &SchemaDeclaration {
        &self.schema
    }
}

impl Create for StatsTable {
    fn table_query(schema: &SchemaDeclaration) -> Result<String, String> {
        Ok(format!(
            "CREATE TABLE {} ({} UNIQUE, {} INTEGER NOT NULL DEFAULT 0);",
            schema.name(),
            Self::COLUMNS[0],
            Self::COLUMNS[1].get_name()
        ))
    }
}

impl Connect for StatsTable {}
impl Drop for StatsTable {}
