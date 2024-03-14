use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::shadow_tables::interface::VirtualTable;
use crate::vtab_interface::vtab_cursor::*;
use crate::{
    operations::{delete::delete, insert::insert, update::update},
    vtab_interface::WhereClause,
};
use sqlite3_ext::query::ToParam;
use sqlite3_ext::{sqlite3_ext_vtab, vtab::VTab};
use sqlite3_ext::{
    vtab::{ChangeInfo, ChangeType, CreateVTab, UpdateVTab, VTabConnection},
    Connection, Result as ExtResult,
};
use sqlite3_ext::{FromValue, Value};

use super::{connect_to_virtual_table, construct_where_clause, create_virtual_table};
/// Represents a metadata table for managing partitions in a SQLite database.
///
/// This structure implements the `VTab` trait to provide custom virtual table functionality,
/// allowing operations such as insert, update, and delete to be performed on partitioned data.
#[derive(Debug)]
#[sqlite3_ext_vtab(StandardModule, UpdateVTab)]
pub struct PartitionMetaTable<'vtab> {
    /// An interface to the partition logic, encapsulating partition management.
    pub interface: VirtualTable<'vtab>,
    /// Reference to the SQLite connection, used for executing SQL statements.
    pub connection: &'vtab Connection,
    /// A map for tracking row IDs provided by the VTab-cursor to their corresponding persisted rowid and what partition it is stored in.
    /// Needed because persisted rowid are only unique within one table, not across multiple
    /// partitions
    pub rowid_mapper: &'vtab RwLock<HashMap<i64, (i64, String)>>,
}
impl<'vtab> CreateVTab<'vtab> for PartitionMetaTable<'vtab> {
    /// Creates a new instance of the partition metadata table.
    ///
    /// This method initializes the partition metadata table, creating the necessary
    /// SQL schema based on provided arguments and establishing a connection to the database.
    fn create(
        db: &'vtab VTabConnection,
        rowid_mapper: &'vtab Self::Aux,
        args: &[&str],
    ) -> ExtResult<(String, Self)>
    where
        Self: Sized,
    {
        // Creation logic for the partition, including SQL table creation
        let virtual_table = match create_virtual_table(db, args) {
            Ok(partition) => partition,
            Err(err) => return Err(err.into()),
        };
        // The schema that serves as a interface to the user.
        let sql = virtual_table.create_table_query();
        Ok((
            sql,
            PartitionMetaTable {
                interface: virtual_table,
                connection: db,
                rowid_mapper,
            },
        ))
    }
    /// Destroys the virtual table, cleaning up all associated resources.
    ///
    /// This includes dropping all partition tables as well as all associated tables,
    /// ensuring a clean state upon deletion
    fn destroy(&mut self) -> ExtResult<()> {
        self.interface.destroy()
    }
}
impl<'vtab> UpdateVTab<'vtab> for PartitionMetaTable<'vtab> {
    /// Handles updates to the virtual table, including inserts, updates, and deletes.
    ///
    /// Based on the type of change (insert, update, delete), this method constructs
    /// the appropriate SQL statements and executes them.
    fn update(&'vtab self, info: &mut ChangeInfo) -> ExtResult<i64> {
        match info.change_type() {
            ChangeType::Insert => insert(&self.interface, info),
            ChangeType::Update => {
                let rowid_mapper = self.rowid_mapper.read().map_err(|e| {
                    sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
                })?;
                let id = info.rowid_mut().get_i64();
                if let Some((db_rowid, partition_name)) = rowid_mapper.get(&id) {
                    let (sql, mut values) =
                        update(partition_name, &self.interface, info.args_mut());
                    let mut stmt = self.connection.prepare(&sql)?;
                    values.iter_mut().enumerate().for_each(|(index, value)| {
                        value.bind_param(&mut stmt, (index + 1) as i32).unwrap();
                    });

                    db_rowid.bind_param(stmt.borrow_mut(), (values.len() + 1) as i32)?;
                    stmt.execute(())?;
                }

                Ok(id)
            }
            ChangeType::Delete => {
                let rowid_mapper = self.rowid_mapper.write().map_err(|e| {
                    sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
                })?;
                let id = info.rowid().get_i64();
                if let Some((db_rowid, partition_name)) = rowid_mapper.get(&id) {
                    let sql = delete(partition_name);
                    let mut stmt = self.connection.prepare(&sql)?;
                    db_rowid.bind_param(stmt.borrow_mut(), 1)?;
                    stmt.execute(())?;
                }

                Ok(id)
            }
        }
    }
}
impl<'vtab> VTab<'vtab> for PartitionMetaTable<'vtab> {
    /// Auxiliary type used by this virtual table, specifically for row ID mapping. This type will
    /// be initialized by the sqlite3 engine.
    type Aux = RwLock<HashMap<i64, (i64, String)>>; //internal rowid. rowid from table, table name
    /// The cursor type used for iterating over partition data.
    type Cursor = RangePartitionCursor<'vtab>;
    /// Connects to the virtual table, initializing it with necessary arguments.
    ///
    /// This method is responsible for setting up the partition metadata table
    /// and preparing it for operation based on provided arguments
    fn connect(
        db: &'vtab VTabConnection,
        rowid_mapper: &'vtab Self::Aux,
        args: &[&str],
    ) -> ExtResult<(String, Self)>
    where
        Self: Sized,
    {
        // Connection logic, similar to `create` but for establishing connections without creating tables.
        let p = match connect_to_virtual_table(db, args[2]) {
            Ok(partition) => partition,
            Err(err) => return Err(err),
        };
        let connection = db;

        Ok((
            p.create_table_query().to_string(),
            PartitionMetaTable {
                interface: p,
                connection,
                rowid_mapper, // rows: None,
            },
        ))
    }
    /// Opens a cursor for accessing the virtual table's data.
    ///
    /// This method initializes and returns a cursor that can be used to query
    /// and manipulate the data within the virtual table.

    fn open(&'vtab self) -> ExtResult<Self::Cursor> {
        Ok(RangePartitionCursor::new(self))
    }
    /// Determines the best index to use for a query on the virtual table.
    ///
    /// Basically builds WHERE clauses to constrain the range of which partition tables to scan, as well
    /// as where clauses to apply to the actual partition tables.

    fn best_index(&self, index_info: &mut sqlite3_ext::vtab::IndexInfo) -> ExtResult<()> {
        let mut argv_index = 0;
        for mut constraint in index_info.constraints() {
            if constraint.usable() {
                constraint.set_argv_index(Some(argv_index));
                argv_index += 1;
            }
        }
        index_info.set_estimated_cost(1.0); // Set a default cost, could be refined.
        let mut where_clauses = construct_where_clause(index_info, &self.interface)?;
        let partitions_where_clauses =
            where_clauses.get(self.interface.lookup().partition_table_column().get_name());

        let partition_column_constraints = partitions_where_clauses.map(|clauses| {
            clauses
                .iter()
                .filter(|clause| clause.get_name() == self.interface.partition_column_name())
                .collect::<Vec<&WhereClause>>()
        });

        let lookup_where_clause = match partition_column_constraints {
            Some(constraints) => constraints
                .iter()
                .map(|constraint| {
                    let wherec = WhereClause {
                        column_name: self
                            .interface
                            .lookup()
                            .partition_value_column()
                            .get_name()
                            .to_owned(),
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
    /// Disconnects from the virtual table, cleaning up resources.
    ///
    /// This is the cleanup counterpart to `connect`, ensuring that any resources
    /// allocated during the operation of the virtual table are properly released.
    fn disconnect(&mut self) -> ExtResult<()> {
        let mut rowid_mapper = self.rowid_mapper.write().map_err(|e| {
            sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
        })?;
        rowid_mapper.clear();
        Ok(())
    }
}
