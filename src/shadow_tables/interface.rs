use std::cell::RefCell;
use std::sync::atomic::{AtomicI64, Ordering};

use sqlite3_ext::query::Statement;
use sqlite3_ext::query::ToParam;
use sqlite3_ext::Connection;
use sqlite3_ext::FallibleIteratorMut;
use sqlite3_ext::FromValue;
use sqlite3_ext::Value;
use sqlite3_ext::ValueRef;
use sqlite3_ext::ValueType;

use crate::companions::{self, Companion, CompanionDecl};
use crate::utils::parse_partition_value;
use crate::ColumnDeclarations;
use crate::LookupTable;
use crate::RootTable;
use crate::StatsTable;
use crate::TemplateTable;

use super::operations::Drop;
use super::operations::Table;

/// Compile-time configurable number of rows buffered per partition before flushing.
/// Set at build time with `PARTITIONER_INSERT_BATCH_SIZE=5000 cargo build`.
pub const INSERT_BATCH_SIZE: usize = match option_env!("PARTITIONER_INSERT_BATCH_SIZE") {
    Some(value) => match_const_usize(value),
    None => 1000,
};

const fn match_const_usize(value: &str) -> usize {
    match parse_const_usize(value.as_bytes()) {
        Some(size) => size,
        None => 1000,
    }
}

const fn parse_const_usize(bytes: &[u8]) -> Option<usize> {
    let mut result: usize = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b < b'0' || b > b'9' {
            return None;
        }
        result = result * 10 + (b - b'0') as usize;
        i += 1;
    }
    Some(result)
}

/// A single row buffered for batch insertion.
#[derive(Debug)]
pub struct PendingRow {
    /// Virtual rowid exposed to SQLite until the row is flushed.
    pub virtual_rowid: i64,
    /// Column values owned by Rust code.
    pub values: Vec<Value>,
}

/// In-memory batch of pending inserts keyed by partition value.
#[derive(Debug)]
pub struct InsertBatch {
    /// Maximum rows to accumulate per partition before eager flush.
    pub batch_size: usize,
    /// Pending rows grouped by partition start value.
    pub pending: RefCell<std::collections::HashMap<i64, Vec<PendingRow>>>,
    /// Source of temporary virtual rowids (negative to avoid colliding with real rowids).
    pub next_virtual_rowid: AtomicI64,
    /// Cache for parsed text partition-column values -> partition start value.
    pub partition_value_cache: RefCell<std::collections::HashMap<String, i64>>,
    /// Partition values whose metadata has already been ensured in the current transaction.
    /// Maps partition start value to the physical partition table name.
    pub partition_names: RefCell<std::collections::HashMap<i64, String>>,
}

impl InsertBatch {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            pending: RefCell::new(std::collections::HashMap::with_capacity(1024)),
            next_virtual_rowid: AtomicI64::new(-1),
            partition_value_cache: RefCell::new(std::collections::HashMap::with_capacity(1024)),
            partition_names: RefCell::new(std::collections::HashMap::with_capacity(1024)),
        }
    }
}

/// Represents a virtual table with partitioning capabilities in SQLite.
///
/// Encapsulates the operations required for managing and interacting with a virtual table,
/// including connecting to existing tables, creating new tables with specific partitioning
/// settings, and performing data manipulation operations like insertions and deletions.
#[derive(Debug)]
pub struct VirtualTable<'vtab> {
    /// Reference to the SQLite database connection.
    pub connection: &'vtab Connection,
    /// Base name of the virtual table.
    base_name: String,
    /// Associated template table for creating new partitions.
    template_table: TemplateTable,
    /// Root table containing metadata about partitions.
    root_table: RootTable,
    /// Lookup table managing the mapping between partition values and partition names.
    lookup_table: LookupTable<i64>,
    /// Stats table storing planner/runtime metadata per partition.
    stats_table: StatsTable,
    /// Cached partition insert statements keyed by physical partition table name.
    insert_statements: RefCell<std::collections::HashMap<String, Statement>>,
    /// Cached multi-row insert statements keyed by (partition_name, chunk_row_count).
    batch_insert_statements: RefCell<std::collections::HashMap<(String, usize), Statement>>,
    /// Buffered rows waiting to be flushed in a batch.
    insert_batch: RefCell<InsertBatch>,
    /// Companion shadow tables kept in sync with the data partitions.
    companions: Vec<Box<dyn Companion>>,
    /// Cached index of the partition column in the declared column order.
    partition_column_index: usize,
}

impl<'vtab> VirtualTable<'vtab> {
    /// Connects to an existing virtual table within the database.
    ///
    /// This function initializes a `VirtualTable` instance by connecting to the existing components
    /// of a virtual table, including the root, template, and lookup tables, based on the provided name.
    /// It enables subsequent operations on the virtual table through the returned `VirtualTable` instance.
    ///
    /// # Parameters
    /// - `db`: A reference to the active database connection.
    /// - `name`: The name of the virtual table to connect to.
    ///
    /// # Returns
    /// Returns a `VirtualTable` instance if the connection is successful, encapsulating the virtual
    /// table's operational context. On failure, returns an error indicating the issue encountered
    /// during the connection process.
    pub fn connect(
        db: &'vtab Connection,
        name: &str,
    ) -> Result<VirtualTable<'vtab>, sqlite3_ext::Error> {
        let root_table = RootTable::connect(db, name)?;
        let template_table = TemplateTable::connect(db, name)?;
        let partition_column_index =
            Self::find_partition_column_index(&template_table, root_table.partition_column());
        let companions = companions::load_decls(db, name)?
            .iter()
            .map(|decl| companions::instantiate(decl, template_table.columns()))
            .collect::<sqlite3_ext::Result<Vec<_>>>()?;
        let table = VirtualTable {
            connection: db,
            base_name: name.to_string(),
            root_table,
            template_table,
            lookup_table: LookupTable::connect(db, name)?,
            stats_table: StatsTable::connect(db, name)?,
            insert_statements: RefCell::new(std::collections::HashMap::new()),
            batch_insert_statements: RefCell::new(std::collections::HashMap::new()),
            insert_batch: RefCell::new(InsertBatch::new(1000)),
            companions,
            partition_column_index,
        };
        Ok(table)
    }

    /// Creates a new instance of a virtual table with specified configurations.
    ///
    /// Initializes and configures a new virtual table in the database, setting up associated structures
    /// like the lookup table for partition mapping, the root table for metadata, and a template table
    /// for defining the structure of partitions. This method facilitates setting up a partitioned virtual
    /// table environment with custom column definitions and partitioning strategy.
    ///
    /// # Parameters
    /// - `db`: A reference to the active database connection.
    /// - `name`: The base name for the virtual table and its associated structures.
    /// - `column_declarations`: Specifications of columns for the virtual table.
    /// - `partition_column`: The name of the column used to determine partitioning.
    /// - `interval`: The interval used for partitioning data.
    ///
    /// # Returns
    /// On success, returns an instance of `VirtualTable`. If any part of the setup fails, an error is returned.
    pub fn create(
        db: &'vtab Connection,
        name: &str,
        column_declarations: ColumnDeclarations,
        partition_column: String,
        interval: i64,
        lifetime_column: Option<i64>,
        companion_decls: &[CompanionDecl],
    ) -> sqlite3_ext::Result<Self> {
        let root_table = RootTable::create(db, name, partition_column, interval, lifetime_column)?;
        let template_table = TemplateTable::create(db, name, column_declarations)?;
        // Index the partition column on the template so every partition inherits it via
        // copy_indices_query. This makes per-partition range filters and ORDER BY on the
        // partition column index scans instead of full partition scans.
        let partition_column_idx_sql = format!(
            "CREATE INDEX {} ON {} ({})",
            TemplateTable::partition_column_index_name(name),
            template_table.name(),
            root_table.partition_column(),
        );
        db.execute(&partition_column_idx_sql, ())?;
        let partition_column_index =
            Self::find_partition_column_index(&template_table, root_table.partition_column());
        let companions = if companion_decls.is_empty() {
            Vec::new()
        } else {
            companions::create_store(db, name)?;
            let mut companions = Vec::with_capacity(companion_decls.len());
            for decl in companion_decls {
                companions::store_decl(db, name, decl)?;
                let companion = companions::instantiate(decl, template_table.columns())?;
                for sql in companion.create_sql(name) {
                    db.execute(&sql, ())?;
                }
                companions.push(companion);
            }
            companions
        };
        Ok(VirtualTable {
            connection: db,
            base_name: name.to_string(),
            lookup_table: LookupTable::create(db, name)?,
            root_table,
            stats_table: StatsTable::create(db, name)?,
            template_table,
            insert_statements: RefCell::new(std::collections::HashMap::new()),
            batch_insert_statements: RefCell::new(std::collections::HashMap::new()),
            insert_batch: RefCell::new(InsertBatch::new(INSERT_BATCH_SIZE)),
            companions,
            partition_column_index,
        })
    }
    /// Destroys the virtual table and all its associated data structures.
    ///
    /// This method deletes all partitions managed by the virtual table, as well as the lookup, root,
    /// and template tables. It ensures a clean removal of all database artifacts related to the virtual table.
    ///
    /// # Returns
    /// On successful execution, returns `Ok(())`. If an error occurs during the deletion of any component,
    /// an error is returned detailing the issue.
    pub fn destroy(&self) -> sqlite3_ext::Result<()> {
        self.flush_all()?;
        for partition in self.lookup_table.get_partitions_by_range(
            self.connection,
            &std::ops::Bound::Unbounded,
            &std::ops::Bound::Unbounded,
        )? {
            self.connection
                .execute(&format!("DROP TABLE {}", partition.1), ())?;
        }
        self.lookup_table.drop_table(self.connection)?;
        self.stats_table.drop_table(self.connection)?;
        for companion in &self.companions {
            for sql in companion.drop_sql(&self.base_name) {
                self.connection.execute(&sql, ())?;
            }
        }
        companions::drop_store(self.connection, &self.base_name)?;
        self.root_table.drop_table(self.connection)?;
        self.template_table.drop_table(self.connection)?;
        Ok(())
    }
    /// Retrieves the name of an existing partition or creates a new partition for the given value.
    ///
    /// This method looks up the partition associated with the provided `partition_value`. If a
    /// partition does not exist, it creates a new partition by copying the template table structure,
    /// updates the lookup table with this new partition's information, and returns the new partition's name.
    ///
    /// # Parameters
    /// * `partition_value` - The value determining which partition to retrieve or create.
    ///
    /// # Returns
    /// The name of the existing or newly created partition as a result. In case of errors during
    /// lookup, creation, or insertion into the lookup table, an appropriate error is returned.
    pub fn get_partition(&self, partition_value: &i64) -> sqlite3_ext::Result<String> {
        self.lookup_table
            .get_partition(partition_value)
            .and_then(|name| match name {
                None => {
                    let new_partition_name = self.copy(&partition_value.to_string())?;
                    let expires_at = self.partition_expiration(*partition_value);
                    self.lookup_table.insert(
                        self.connection,
                        &new_partition_name,
                        *partition_value,
                        expires_at,
                    )?;
                    self.stats_table
                        .insert_partition(self.connection, &new_partition_name)?;
                    Ok(new_partition_name)
                }
                Some(name) => Ok(name.to_owned()),
            })
    }

    fn partition_expiration(&self, partition_value: i64) -> Option<i64> {
        self.root_table
            .get_lifetime()
            .map(|lifetime| partition_value + self.partition_interval() + lifetime)
    }

    /// Copies the template table structure to create a new partition table with a specified suffix.
    ///
    /// # Parameters
    /// * `suffix` - The suffix to append to the base name for the new partition table.
    ///
    /// # Returns
    /// The name of the newly created partition table.
    fn copy(&self, suffix: &str) -> sqlite3_ext::Result<String> {
        let new_table_name = self.format_new_table_name(suffix);
        self.template_table.copy(&new_table_name, self.connection)?;
        self.template_table
            .copy_indices_query(self.connection, &new_table_name)?;
        Ok(new_table_name)
    }

    /// Generates a new table name by appending a suffix to the virtual table's base name.
    ///
    /// # Parameters
    /// * `suffix` - The suffix to be appended.
    ///
    /// # Returns
    /// The formatted new table name.
    fn format_new_table_name(&self, suffix: &str) -> String {
        format!("{}_{}", self.base_name, suffix)
    }

    /// Retrieves the SQL query to create a table based on the template table's schema.
    ///
    /// # Returns
    /// The SQL CREATE TABLE query string.
    pub fn create_table_query(&self) -> String {
        let mut interface_schema = self.template_table.schema().clone();
        // let mut hidden_column =
        //     ColumnDeclaration::new("_partition".to_string(), sqlite3_ext::ValueType::Text);
        // hidden_column.set_hidden();
        interface_schema.name = self.base_name.clone();
        // interface_schema.columns.0.push(hidden_column);
        interface_schema.table_query()
    }

    /// Accesses the column declarations of the template table.
    ///
    /// # Returns
    /// A reference to the `ColumnDeclarations` of the template table.
    pub fn columns(&self) -> &ColumnDeclarations {
        self.template_table.columns()
    }

    /// Retrieves the name of the partition column from the root table.
    ///
    /// # Returns
    /// The name of the partition column.
    fn find_partition_column_index(
        template_table: &TemplateTable,
        partition_column_name: &str,
    ) -> usize {
        template_table
            .columns()
            .0
            .iter()
            .position(|column| column.get_name() == partition_column_name)
            .unwrap_or(0)
    }

    pub fn partition_column_name(&self) -> &str {
        self.root_table.partition_column()
    }

    /// Base name of the virtual table.
    pub fn base_name(&self) -> &str {
        &self.base_name
    }

    pub fn partition_column_index(&self) -> usize {
        self.partition_column_index
    }

    /// Retrieves the partition interval set in the root table.
    ///
    /// # Returns
    /// The partition interval in seconds.
    pub fn partition_interval(&self) -> i64 {
        self.root_table.get_interval()
    }

    /// Compute the partition start value for a partition column value, caching text parses.
    pub fn partition_value_for(&self, value: &ValueRef) -> sqlite3_ext::Result<i64> {
        let interval = self.partition_interval();
        if value.value_type() == ValueType::Text {
            let text = value.try_get_str().map_err(|_| {
                sqlite3_ext::Error::Module("Invalid UTF-8 in partition column".into())
            })?;
            if let Some(cached) = self
                .insert_batch
                .borrow()
                .partition_value_cache
                .borrow()
                .get(text)
            {
                return Ok(*cached);
            }
            let partition_value = parse_partition_value(value, interval)?;
            self.insert_batch
                .borrow()
                .partition_value_cache
                .borrow_mut()
                .insert(text.to_string(), partition_value);
            Ok(partition_value)
        } else {
            parse_partition_value(value, interval)
        }
    }

    pub fn lifetime(&self) -> Option<i64> {
        self.root_table.get_lifetime()
    }

    /// Provides a reference to the lookup table associated with the virtual table.
    ///
    /// # Returns
    /// A reference to the `LookupTable`.
    pub fn lookup(&self) -> &LookupTable<i64> {
        &self.lookup_table
    }

    pub fn stats(&self) -> &StatsTable {
        &self.stats_table
    }

    /// Companion shadow tables declared for this virtual table.
    pub fn companions(&self) -> &[Box<dyn Companion>] {
        &self.companions
    }

    /// Resolves a physical partition table name back to its partition value.
    pub fn partition_value_of(&self, partition_name: &str) -> sqlite3_ext::Result<Option<i64>> {
        let sql = format!(
            "SELECT {} FROM {} WHERE {} = ?",
            self.lookup_table.partition_value_column().get_name(),
            self.lookup_table.name(),
            self.lookup_table.partition_table_column().get_name(),
        );
        let mut stmt = self.connection.prepare(&sql)?;
        let rows = stmt.query([partition_name])?;
        match rows.next() {
            Ok(Some(row)) => Ok(Some(row[0].get_i64())),
            _ => Ok(None),
        }
    }

    pub fn row_count_for_range(
        &self,
        lower_bound: &std::ops::Bound<i64>,
        upper_bound: &std::ops::Bound<i64>,
    ) -> sqlite3_ext::Result<i64> {
        let partitions =
            self.lookup_table
                .get_partitions_by_range(self.connection, lower_bound, upper_bound)?;
        partitions
            .iter()
            .try_fold(0i64, |rows, (_, partition_name)| {
                Ok(rows
                    + self
                        .stats_table
                        .row_count(partition_name)?
                        .unwrap_or_default())
            })
    }

    /// Inserts a new row into the appropriate partition based on the specified partition value.
    ///
    /// # Parameters
    /// * `partition_value` - The value determining which partition the new row belongs to.
    /// * `columns` - An array of references to `ValueRef`, representing the values to be inserted.
    ///
    /// # Returns
    /// The ROWID of the inserted row.
    pub fn insert(&self, partition_value: i64, columns: &[&ValueRef]) -> sqlite3_ext::Result<i64> {
        let partition = self.get_partition(&partition_value)?;
        let rowid = {
            let mut cached_statements = self.insert_statements.borrow_mut();
            let stmt = if let Some(stmt) = cached_statements.get_mut(&partition) {
                stmt
            } else {
                let placeholders = std::iter::repeat("?")
                    .take(columns.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!("INSERT INTO {} VALUES({})", partition, placeholders);
                cached_statements
                    .entry(partition.clone())
                    .or_insert(self.connection.prepare(&sql)?)
            };
            for (index, column) in columns.iter().enumerate() {
                column.bind_param(stmt, (index + 1) as i32)?;
            }
            stmt.insert(())?
        };
        self.stats_table
            .increment_row_count(self.connection, &partition)?;
        for companion in &self.companions {
            companion.on_row_inserted(
                self.connection,
                &self.base_name,
                partition_value,
                rowid,
                columns,
            )?;
        }
        Ok(rowid)
    }

    /// Buffer a row for batch insertion, returning a temporary virtual rowid.
    ///
    /// If the target partition's buffer reaches the batch size, that partition is flushed
    /// immediately.
    pub fn buffer_insert(
        &self,
        partition_value: i64,
        columns: &[&ValueRef],
    ) -> sqlite3_ext::Result<i64> {
        let virtual_rowid = self
            .insert_batch
            .borrow()
            .next_virtual_rowid
            .fetch_sub(1, Ordering::SeqCst);
        let values = columns
            .iter()
            .map(|col| FromValue::to_owned(*col))
            .collect::<sqlite3_ext::Result<Vec<Value>>>()?;

        // Ensure the partition table and its metadata exist before buffering the row.
        // This is cheap because each partition is only created once per batch.
        {
            let pending = self.insert_batch.borrow();
            if !pending
                .partition_names
                .borrow()
                .contains_key(&partition_value)
            {
                let partition_name = self.get_partition(&partition_value)?;
                pending
                    .partition_names
                    .borrow_mut()
                    .insert(partition_value, partition_name);
            }
        }

        let should_flush = {
            let pending = self.insert_batch.borrow_mut();
            let batch_size = pending.batch_size;
            let buffer_len = {
                let mut map = pending.pending.borrow_mut();
                let buffer = map
                    .entry(partition_value)
                    .or_insert_with(|| Vec::with_capacity(batch_size));
                buffer.push(PendingRow {
                    virtual_rowid,
                    values,
                });
                buffer.len()
            };
            buffer_len >= batch_size
        };

        if should_flush {
            self.flush_partition(partition_value)?;
        }

        Ok(virtual_rowid)
    }

    /// Flush all pending rows for a single partition value.
    ///
    /// Rows are inserted using chunked multi-row `INSERT` statements to reduce the number of
    /// prepared-statement executions. The chunk size respects SQLite's default limit on bound
    /// parameters per statement (999), rounded down conservatively.
    fn flush_partition(&self, partition_value: i64) -> sqlite3_ext::Result<()> {
        let rows = {
            let pending = self.insert_batch.borrow_mut();
            let rows = pending.pending.borrow_mut().remove(&partition_value);
            rows
        };
        let mut rows = match rows {
            Some(rows) if !rows.is_empty() => rows,
            _ => return Ok(()),
        };

        let partition_name = {
            let pending = self.insert_batch.borrow();
            let cached = pending
                .partition_names
                .borrow()
                .get(&partition_value)
                .cloned();
            drop(pending);
            match cached {
                Some(name) => name,
                None => self.get_partition(&partition_value)?,
            }
        };
        let row_count = rows.len() as i64;
        let column_count = rows[0].values.len().max(1);
        // Leave headroom below SQLite's default SQLITE_MAX_VARIABLE_NUMBER (999).
        const MAX_PARAMS: usize = 900;
        let chunk_size = (MAX_PARAMS / column_count).max(1);

        while !rows.is_empty() {
            let at = chunk_size.min(rows.len());
            let chunk: Vec<PendingRow> = rows.drain(..at).collect();

            let row_placeholders = std::iter::repeat("?")
                .take(column_count)
                .collect::<Vec<_>>()
                .join(",");
            let values_clause = (0..chunk.len())
                .map(|_| format!("({})", row_placeholders))
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!("INSERT INTO {} VALUES {}", partition_name, values_clause);

            let mut cached_statements = self.batch_insert_statements.borrow_mut();
            let stmt = cached_statements
                .entry((partition_name.clone(), chunk.len()))
                .or_insert(self.connection.prepare(&sql)?);
            let mut position = 1i32;
            for pending_row in &chunk {
                for value in &pending_row.values {
                    value.clone().bind_param(stmt, position)?;
                    position += 1;
                }
            }
            stmt.execute(())?;

            if !self.companions.is_empty() {
                // Chunk inserts into a rowid table without explicit rowids produce
                // consecutive rowids ending at last_insert_rowid().
                let last_rowid: i64 = self
                    .connection
                    .query_row("SELECT last_insert_rowid()", (), |row| Ok(row[0].get_i64()))?;
                let first_rowid = last_rowid - chunk.len() as i64 + 1;
                for companion in &self.companions {
                    companion.on_rows_flushed(
                        self.connection,
                        &self.base_name,
                        partition_value,
                        first_rowid,
                        &chunk,
                    )?;
                }
            }
        }

        self.stats_table
            .increment_row_count_by(self.connection, &partition_name, row_count)?;
        Ok(())
    }

    /// Flush every pending partition buffer.
    pub fn flush_all(&self) -> sqlite3_ext::Result<()> {
        let keys: Vec<i64> = self
            .insert_batch
            .borrow()
            .pending
            .borrow()
            .keys()
            .copied()
            .collect();
        for partition_value in keys {
            self.flush_partition(partition_value)?;
        }
        Ok(())
    }

    /// Drop all pending rows without persisting them.
    pub fn clear_pending(&self) {
        let pending = self.insert_batch.borrow_mut();
        pending.pending.borrow_mut().clear();
        pending.partition_names.borrow_mut().clear();
    }
}

#[cfg(test)]
mod tests {

    use std::ops::IndexMut;

    use crate::{utils::parse_interval, PartitionColumn};

    use super::*;
    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::{Connection, FromValue};
    fn mock_template() -> (String, ColumnDeclarations, PartitionColumn, i64) {
        let columns = ColumnDeclarations::from_iter(&[
            "first_column timestamp partition_column",
            "second_column int",
            "third_column varchar",
        ]);
        let partition_column = PartitionColumn::from_iter(columns.clone());
        let interval = parse_interval("1 hour").unwrap();
        ("test".to_string(), columns, partition_column, interval)
    }

    fn create_virtual_table<'test>(conn: &'test Connection) -> VirtualTable<'test> {
        let (name, columns, partition_column, interval) = mock_template();
        let partition_column_name = partition_column.column_def().as_ref().unwrap().get_name();
        let table = VirtualTable::create(
            conn,
            &name,
            columns,
            partition_column_name.to_string(),
            interval,
            None,
            &[],
        );
        assert!(table.is_ok());
        table.unwrap()
    }

    fn create_virtual_table_with_lifetime<'test>(
        conn: &'test Connection,
        lifetime: i64,
    ) -> VirtualTable<'test> {
        let (name, columns, partition_column, interval) = mock_template();
        let partition_column_name = partition_column.column_def().as_ref().unwrap().get_name();
        let table = VirtualTable::create(
            conn,
            &name,
            columns,
            partition_column_name.to_string(),
            interval,
            Some(lifetime),
            &[],
        );
        assert!(table.is_ok());
        table.unwrap()
    }
    #[test]
    fn test_create_virtual_table() {
        let conn = match RusqConn::open_in_memory() {
            Ok(conn) => conn,
            Err(err) => panic!("{}", err.to_string()),
        };
        let conn = Connection::from_rusqlite(&conn);

        let virtual_table = create_virtual_table(conn);
        assert_eq!(
            virtual_table.create_table_query().to_lowercase(),
            "create table test (first_column text, second_column integer, third_column text)"
        )
    }

    #[test]
    fn test_partition_expiration_uses_interval_plus_lifetime() -> sqlite3_ext::Result<()> {
        let conn = RusqConn::open_in_memory().unwrap();
        let conn = Connection::from_rusqlite(&conn);
        let virtual_table = create_virtual_table_with_lifetime(conn, 24 * 60 * 60);
        let partition_value = parse_interval("1 hour").unwrap();

        let partition_name = virtual_table.get_partition(&partition_value)?;
        assert_eq!(partition_name, "test_3600");

        conn.query_row(
            "SELECT expires_at FROM test_lookup WHERE partition_table = ?",
            [partition_name.as_str()],
            |row| {
                let expires_at = row.index_mut(0).get_i64();
                assert_eq!(
                    expires_at,
                    partition_value + parse_interval("1 hour").unwrap() + 24 * 60 * 60
                );
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_partition_without_lifetime_has_null_expiration() -> sqlite3_ext::Result<()> {
        let conn = RusqConn::open_in_memory().unwrap();
        let conn = Connection::from_rusqlite(&conn);
        let virtual_table = create_virtual_table(conn);
        let partition_name = virtual_table.get_partition(&3600)?;

        conn.query_row(
            "SELECT expires_at IS NULL FROM test_lookup WHERE partition_table = ?",
            [partition_name.as_str()],
            |row| {
                assert_eq!(row.index_mut(0).get_i64(), 1);
                Ok(())
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_new_partition_copies_template_indexes() -> sqlite3_ext::Result<()> {
        let conn = RusqConn::open_in_memory().unwrap();
        let conn = Connection::from_rusqlite(&conn);
        let virtual_table = create_virtual_table(conn);

        conn.execute(
            "CREATE INDEX test_first_idx ON test_template(first_column)",
            (),
        )?;
        conn.execute(
            "CREATE INDEX test_second_idx ON test_template(second_column)",
            (),
        )?;

        let partition_name = virtual_table.get_partition(&3600)?;
        let copied_index_count = conn.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND tbl_name = ?",
            [partition_name.as_str()],
            |row| Ok(row.index_mut(0).get_i64()),
        )?;

        // Two custom indexes plus the partition-column index added at create time.
        assert_eq!(copied_index_count, 3);

        Ok(())
    }
}
