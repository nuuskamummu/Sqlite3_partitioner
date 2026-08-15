use std::borrow::BorrowMut;
use std::ops::Bound;
use std::sync::RwLock;

use crate::constraints::{ScanPlan, SortDirection, WhereClause};
use crate::operations::{delete::delete, insert::insert, update::update};
use crate::shadow_tables::interface::VirtualTable;
use crate::utils::validation::validate_and_map_columns;
use crate::utils::{parse_partition_value, parse_to_unix_epoch};
use crate::vtab_interface::vtab_cursor::*;
use sqlite3_ext::query::ToParam;
use sqlite3_ext::FromValue;
use sqlite3_ext::{ffi, sqlite3_ext_vtab, vtab::VTab};
use sqlite3_ext::{
    vtab::{
        ChangeInfo, ChangeType, ConstraintOp, CreateVTab, TransactionVTab, UpdateVTab,
        VTabConnection, VTabTransaction,
    },
    Connection, Result as ExtResult,
};

use super::{connect_to_virtual_table, construct_where_clause, create_virtual_table};

#[derive(Clone, Copy, Debug, Default)]
struct PartitionConstraintShape {
    has_eq: bool,
    has_lower: bool,
    has_upper: bool,
}

impl PartitionConstraintShape {
    fn from_constraints(constraints: &[&WhereClause]) -> Self {
        let mut shape = Self::default();
        for constraint in constraints {
            match constraint.get_operator() {
                sqlite3_ext::vtab::ConstraintOp::Eq => shape.has_eq = true,
                sqlite3_ext::vtab::ConstraintOp::GT | sqlite3_ext::vtab::ConstraintOp::GE => {
                    shape.has_lower = true
                }
                sqlite3_ext::vtab::ConstraintOp::LT | sqlite3_ext::vtab::ConstraintOp::LE => {
                    shape.has_upper = true
                }
                _ => {}
            }
        }
        shape
    }
}

#[derive(Clone, Copy, Debug)]
struct LiteralPartitionEstimate {
    partitions: i64,
    rows: i64,
}

fn estimate_rows_for_partition_range(
    interface: &VirtualTable,
    lower_bound: &Bound<i64>,
    upper_bound: &Bound<i64>,
) -> ExtResult<LiteralPartitionEstimate> {
    let partitions = interface.lookup().get_partitions_by_range(
        interface.connection,
        lower_bound,
        upper_bound,
    )?;
    let mut rows = 0i64;
    for (_, partition_name) in partitions.iter() {
        rows += interface
            .stats()
            .row_count(partition_name)?
            .unwrap_or_default();
    }

    Ok(LiteralPartitionEstimate {
        partitions: partitions.len() as i64,
        rows,
    })
}

fn more_restrictive_bound(a: Bound<i64>, b: Bound<i64>) -> Bound<i64> {
    match (a, b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        (Bound::Included(a_val), Bound::Included(b_val)) => {
            Bound::Included(std::cmp::max(a_val, b_val))
        }
        (Bound::Excluded(a_val), Bound::Excluded(b_val)) => {
            Bound::Excluded(std::cmp::max(a_val, b_val))
        }
        (Bound::Excluded(a_val), Bound::Included(b_val))
        | (Bound::Included(a_val), Bound::Excluded(b_val)) => {
            if a_val >= b_val {
                Bound::Excluded(a_val)
            } else {
                Bound::Included(b_val)
            }
        }
    }
}

fn initial_partition_bounds(
    operator: ConstraintOp,
    epoch: i64,
    partition_start: i64,
    interval: i64,
) -> (Bound<i64>, Bound<i64>) {
    let is_exact_boundary = epoch.rem_euclid(interval) == 0;
    match operator {
        ConstraintOp::GT | ConstraintOp::GE => (Bound::Included(partition_start), Bound::Unbounded),
        ConstraintOp::LT => (
            Bound::Unbounded,
            if is_exact_boundary {
                Bound::Excluded(partition_start)
            } else {
                Bound::Included(partition_start)
            },
        ),
        ConstraintOp::LE => (Bound::Unbounded, Bound::Included(partition_start)),
        ConstraintOp::Eq => (
            Bound::Included(partition_start),
            Bound::Included(partition_start),
        ),
        _ => (Bound::Unbounded, Bound::Unbounded),
    }
}

fn update_partition_bounds(
    range: &mut (Bound<i64>, Bound<i64>),
    operator: ConstraintOp,
    epoch: i64,
    partition_start: i64,
    interval: i64,
) {
    let is_exact_boundary = epoch.rem_euclid(interval) == 0;
    match operator {
        ConstraintOp::GT | ConstraintOp::GE => {
            range.0 = more_restrictive_bound(range.0, Bound::Included(partition_start));
        }
        ConstraintOp::LT => {
            let upper_bound = if is_exact_boundary {
                Bound::Excluded(partition_start)
            } else {
                Bound::Included(partition_start)
            };
            range.1 = more_restrictive_bound(range.1, upper_bound);
        }
        ConstraintOp::LE => {
            range.1 = more_restrictive_bound(range.1, Bound::Included(partition_start));
        }
        ConstraintOp::Eq => {
            let bound = Bound::Included(partition_start);
            range.0 = more_restrictive_bound(range.0, bound);
            range.1 = more_restrictive_bound(range.1, bound);
        }
        _ => {}
    }
}

fn literal_partition_estimate(
    index_info: &sqlite3_ext::vtab::IndexInfo,
    interface: &VirtualTable,
) -> ExtResult<Option<LiteralPartitionEstimate>> {
    let mut range: Option<(Bound<i64>, Bound<i64>)> = None;
    let mut saw_partition_literal = false;

    for constraint in index_info
        .constraints()
        .filter(|constraint| constraint.usable() && is_row_constraint(constraint))
    {
        let column_index = constraint.column();
        let column = match interface.columns().0.get(column_index as usize) {
            Some(column) => column,
            None => continue,
        };
        if column.get_name() != interface.partition_column_name() {
            continue;
        }
        let rhs = match constraint.rhs() {
            Ok(rhs) => rhs,
            Err(_) => return Ok(None),
        };
        let epoch = match parse_to_unix_epoch(rhs) {
            Ok(epoch) => epoch,
            Err(_) => return Ok(None),
        };
        let partition_start = epoch - epoch.rem_euclid(interface.partition_interval());
        saw_partition_literal = true;

        if let Some(existing_range) = &mut range {
            update_partition_bounds(
                existing_range,
                constraint.op(),
                epoch,
                partition_start,
                interface.partition_interval(),
            );
        } else {
            range = Some(initial_partition_bounds(
                constraint.op(),
                epoch,
                partition_start,
                interface.partition_interval(),
            ));
        }
    }

    if !saw_partition_literal {
        return Ok(None);
    }

    let (lower_bound, upper_bound) = range.unwrap_or((Bound::Unbounded, Bound::Unbounded));
    Ok(Some(estimate_rows_for_partition_range(
        interface,
        &lower_bound,
        &upper_bound,
    )?))
}
/// True for constraints that are real row predicates the vtab can enforce.
/// LIMIT/OFFSET pseudo-constraints (SQLite 3.38+) are excluded: they are not row
/// filters, and SQLite enforces them itself regardless.
pub(crate) fn is_row_constraint(constraint: &sqlite3_ext::vtab::IndexInfoConstraint) -> bool {
    constraint.column() >= 0
        && !matches!(constraint.op(), ConstraintOp::Limit | ConstraintOp::Offset)
}

/// Represents a metadata table for managing partitions in a SQLite database.
///
/// This structure implements the `VTab` trait to provide custom virtual table functionality,
/// allowing operations such as insert, update, and delete to be performed on partitioned data.
#[derive(Debug)]
#[sqlite3_ext_vtab(StandardModule, UpdateVTab, TransactionVTab)]
pub struct PartitionMetaTable<'vtab> {
    /// An interface to the partition logic, encapsulating partition management.
    pub interface: VirtualTable<'vtab>,
    /// Reference to the SQLite connection, used for executing SQL statements.
    pub connection: &'vtab Connection,
    /// A map for tracking row IDs provided by the VTab-cursor to their corresponding persisted rowid and what partition it is stored in.
    /// Needed because persisted rowid are only unique within one table, not across multiple
    /// partitions
    pub rowid_mapper: &'vtab RwLock<Vec<(i64, String)>>,
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
    fn destroy(self) -> sqlite3_ext::vtab::DisconnectResult<Self> {
        if let Err(err) = self.interface.flush_all() {
            return Err((self, err));
        }
        if let Err(err) = self.interface.destroy() {
            return Err((self, err));
        }
        Ok(())
    }
}

pub struct PartitionMetaTableTransaction<'vtab> {
    vtab: &'vtab PartitionMetaTable<'vtab>,
    flush_on_commit: bool,
}

impl<'vtab> TransactionVTab<'vtab> for PartitionMetaTable<'vtab> {
    type Transaction = PartitionMetaTableTransaction<'vtab>;

    fn begin(&'vtab self) -> ExtResult<Self::Transaction> {
        let autocommit = unsafe { ffi::sqlite3_get_autocommit(self.connection.as_mut_ptr()) != 0 };
        Ok(PartitionMetaTableTransaction {
            vtab: self,
            flush_on_commit: !autocommit,
        })
    }
}

impl<'vtab> PartitionMetaTableTransaction<'vtab> {
    /// Returns true when SQLite is in autocommit mode, meaning each statement is its own
    /// transaction. In that mode we defer flushing so that many single-row inserts can be
    /// batched together; otherwise we flush at the end of an explicit transaction.
    #[allow(dead_code)]
    fn in_autocommit(&self) -> bool {
        unsafe { ffi::sqlite3_get_autocommit(self.vtab.connection.as_mut_ptr()) != 0 }
    }
}

impl<'vtab> VTabTransaction for PartitionMetaTableTransaction<'vtab> {
    fn sync(&mut self) -> ExtResult<()> {
        if self.flush_on_commit {
            self.vtab.interface.flush_all()
        } else {
            Ok(())
        }
    }

    fn commit(self) -> ExtResult<()> {
        if self.flush_on_commit {
            self.vtab.interface.flush_all()
        } else {
            Ok(())
        }
    }

    fn rollback(self) -> ExtResult<()> {
        self.vtab.interface.clear_pending();
        Ok(())
    }

    fn savepoint(&mut self, _n: i32) -> ExtResult<()> {
        if self.flush_on_commit {
            self.vtab.interface.flush_all()
        } else {
            Ok(())
        }
    }

    fn release(&mut self, _n: i32) -> ExtResult<()> {
        Ok(())
    }

    fn rollback_to(&mut self, _n: i32) -> ExtResult<()> {
        self.vtab.interface.clear_pending();
        Ok(())
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
                self.interface.flush_all()?;
                let rowid_mapper = self.rowid_mapper.read().map_err(|e| {
                    sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
                })?;
                let id = info.rowid_mut().get_i64();
                if let Some((db_rowid, partition_name)) = rowid_mapper.get(id as usize) {
                    let cols = &info.args()[1..];
                    let column_refs = cols.iter().map(|value| &**value).collect::<Vec<_>>();
                    let (_validated_columns, partition_column) = validate_and_map_columns(
                        column_refs.as_slice(),
                        self.interface.columns().into(),
                        self.interface.partition_column_name(),
                    )?;

                    let target_partition_name = match partition_column {
                        Some(partition_column) => {
                            let partition_value = parse_partition_value(
                                partition_column,
                                self.interface.partition_interval(),
                            )?;
                            self.interface.get_partition(&partition_value)?
                        }
                        None => partition_name.to_string(),
                    };

                    if target_partition_name != *partition_name {
                        self.interface.insert(
                            parse_partition_value(
                                partition_column.ok_or_else(|| {
                                    sqlite3_ext::Error::Module(
                                        "Partition column not found during update".to_string(),
                                    )
                                })?,
                                self.interface.partition_interval(),
                            )?,
                            column_refs.as_slice(),
                        )?;
                        let sql = delete(partition_name);
                        let mut stmt = self.connection.prepare(&sql)?;
                        db_rowid.bind_param(stmt.borrow_mut(), 1)?;
                        stmt.execute(())?;
                        self.interface
                            .stats()
                            .decrement_row_count(self.connection, partition_name)?;
                    } else {
                        let (sql, mut values) =
                            update(partition_name, &self.interface, info.args_mut())?;
                        let mut stmt = self.connection.prepare(&sql)?;
                        values
                            .iter_mut()
                            .enumerate()
                            .try_for_each(|(index, value)| {
                                value.bind_param(&mut stmt, (index + 1) as i32)
                            })?;

                        db_rowid.bind_param(stmt.borrow_mut(), (values.len() + 1) as i32)?;
                        stmt.execute(())?;
                    }
                }

                Ok(id)
            }
            ChangeType::Delete => {
                self.interface.flush_all()?;
                let rowid_mapper = self.rowid_mapper.write().map_err(|e| {
                    sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
                })?;
                let id = info.rowid().get_i64();
                if let Some((db_rowid, partition_name)) = rowid_mapper.get(id as usize) {
                    let sql = delete(partition_name);
                    let mut stmt = self.connection.prepare(&sql)?;
                    db_rowid.bind_param(stmt.borrow_mut(), 1)?;
                    stmt.execute(())?;
                    self.interface
                        .stats()
                        .decrement_row_count(self.connection, partition_name)?;
                }

                Ok(id)
            }
        }
    }
}
impl<'vtab> VTab<'vtab> for PartitionMetaTable<'vtab> {
    /// Auxiliary type used by this virtual table, specifically for row ID mapping. This type will
    /// be initialized by the sqlite3 engine.
    type Aux = RwLock<Vec<(i64, String)>>; //internal rowid. rowid from table, table name
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
        let p = connect_to_virtual_table(db, args[2])?;
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
        // INVARIANT: every constraint handed an argv_index here is fully enforced by
        // xFilter — partition-column constraints via lookup-range pruning AND literal
        // per-partition WHERE clauses, all other constraints via per-partition WHERE
        // clauses. We therefore set omit=true so SQLite does not redundantly re-check
        // each row. If xFilter ever stops applying a constraint, its omit must go.
        let mut argv_index = 0;
        for mut constraint in index_info.constraints() {
            if constraint.usable() && is_row_constraint(&constraint) {
                constraint.set_argv_index(Some(argv_index));
                constraint.set_omit(true);
                argv_index += 1;
            }
        }
        let mut where_clauses = construct_where_clause(index_info, &self.interface)?;
        let partitions_where_clauses =
            where_clauses.get(self.interface.lookup().partition_table_column().get_name());

        let partition_column_constraints = partitions_where_clauses.map(|clauses| {
            clauses
                .iter()
                .filter(|clause| clause.get_name() == self.interface.partition_column_name())
                .collect::<Vec<&WhereClause>>()
        });

        let partition_constraint_shape = partition_column_constraints
            .as_ref()
            .map(|constraints| PartitionConstraintShape::from_constraints(constraints))
            .unwrap_or_default();

        let lookup_where_clause = match partition_column_constraints {
            Some(constraints) => constraints
                .iter()
                .map(|constraint| {
                    Some(WhereClause::new(
                        self.interface
                            .lookup()
                            .partition_value_column()
                            .get_name()
                            .to_owned(),
                        constraint.get_operator().to_owned(),
                        constraint.get_constraint_index(),
                    ))
                })
                .collect::<Option<Vec<WhereClause>>>(),
            None => None,
        };

        lookup_where_clause
            .and_then(|clause| where_clauses.insert("lookup_table".to_string(), clause));

        let total_rows = self.interface.stats().total_rows()?;
        let partition_count = self.interface.stats().partition_count()? as i64;
        let avg_rows_per_partition = if partition_count > 0 {
            ((total_rows + partition_count - 1) / partition_count).max(1)
        } else {
            1
        };
        let fallback_estimated_partitions = if partition_constraint_shape.has_eq {
            1
        } else if partition_constraint_shape.has_lower && partition_constraint_shape.has_upper {
            std::cmp::max(1, (partition_count + 7) / 8)
        } else if partition_constraint_shape.has_lower || partition_constraint_shape.has_upper {
            std::cmp::max(1, (partition_count + 1) / 2)
        } else {
            std::cmp::max(1, partition_count)
        };
        let fallback_estimated_rows = fallback_estimated_partitions
            .saturating_mul(avg_rows_per_partition)
            .max(1);
        let literal_estimate = literal_partition_estimate(index_info, &self.interface)?;
        let estimated_partitions = literal_estimate
            .map(|estimate| estimate.partitions)
            .unwrap_or(fallback_estimated_partitions)
            .max(1);
        let estimated_rows = literal_estimate
            .map(|estimate| estimate.rows)
            .unwrap_or(fallback_estimated_rows)
            .max(1);
        let predicate_groups = where_clauses.len().max(1) as i64;
        let mut estimated_cost =
            estimated_partitions as f64 * 10.0 + (estimated_rows as f64 / predicate_groups as f64);

        // Partitions are disjoint, sorted ranges of the partition column, so scanning
        // partitions in order with each partition's rows ordered by that column yields a
        // globally sorted result. We can therefore consume a single-term ORDER BY on the
        // partition column.
        let partition_order = {
            let order_terms: Vec<_> = index_info.order_by().collect();
            match order_terms.as_slice() {
                [term] if term.column() == self.interface.partition_column_index() as i32 => {
                    Some(if term.desc() {
                        SortDirection::Desc
                    } else {
                        SortDirection::Asc
                    })
                }
                _ => None,
            }
        };
        if partition_order.is_some() {
            index_info.set_order_by_consumed(true);
            // No external sort needed: drop the per-partition surcharge from the cost.
            estimated_cost = estimated_rows as f64 / predicate_groups as f64;
        }

        index_info.set_estimated_rows(estimated_rows);
        index_info.set_estimated_cost(estimated_cost);

        let index_str = ron::to_string(&ScanPlan::new(where_clauses, partition_order))
            .map_err(|err| sqlite3_ext::Error::Module(err.to_string()))?;
        index_info.set_index_str(Some(&index_str))?;

        Ok(())
    }
    /// Disconnects from the virtual table, cleaning up resources.
    ///
    /// This is the cleanup counterpart to `connect`, ensuring that any resources
    /// allocated during the operation of the virtual table are properly released.
    fn disconnect(self) -> sqlite3_ext::vtab::DisconnectResult<Self> {
        if let Err(err) = self.interface.flush_all() {
            return Err((self, err));
        }
        if let Err(err) = self
            .rowid_mapper
            .write()
            .map(|mut mapper| mapper.clear())
            .map_err(|e| {
                sqlite3_ext::Error::Sqlite(1, Some(format!("Lock acquisition failed: {}", e)))
            })
        {
            return Err((self, err));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use rusqlite::Connection as RusqConn;
    use sqlite3_ext::{Connection, TransactionType};

    use crate::vtab_interface::init;

    use super::estimate_rows_for_partition_range;
    use crate::shadow_tables::interface::VirtualTable;

    fn create_virtual_table<'test>(conn: &'test Connection) -> VirtualTable<'test> {
        init(conn).unwrap();
        conn.execute(
            "CREATE VIRTUAL TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 text)",
            (),
        )
        .unwrap();
        VirtualTable::connect(conn, "test").unwrap()
    }

    #[test]
    fn test_estimate_rows_for_partition_range_uses_stats() -> sqlite3_ext::Result<()> {
        let conn = RusqConn::open_in_memory().unwrap();
        let conn = Connection::from_rusqlite(&conn);
        create_virtual_table(conn);
        let txn = conn.transaction(TransactionType::Immediate)?;
        txn.insert(
            "INSERT INTO test VALUES ('2024-01-01 12:15', 'a'), ('2024-01-01 12:45', 'b'), ('2024-01-01 13:15', 'c')",
            (),
        )?;
        txn.commit()?;
        let virtual_table = VirtualTable::connect(conn, "test")?;

        let estimate = estimate_rows_for_partition_range(
            &virtual_table,
            &Bound::Included(1704110400),
            &Bound::Included(1704114000),
        )?;
        assert_eq!(estimate.partitions, 2);
        assert_eq!(estimate.rows, 3);

        let single_partition = estimate_rows_for_partition_range(
            &virtual_table,
            &Bound::Included(1704110400),
            &Bound::Included(1704110400),
        )?;
        assert_eq!(single_partition.partitions, 1);
        assert_eq!(single_partition.rows, 2);

        Ok(())
    }
}
