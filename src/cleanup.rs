use std::time::{SystemTime, UNIX_EPOCH};

use sqlite3_ext::{Connection, Result as ExtResult};

use crate::shadow_tables::interface::VirtualTable;

pub fn cleanup_expired_partitions(db: &Connection, table_name: &str) -> ExtResult<i64> {
    let now_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let vtab = VirtualTable::connect(db, table_name)?;
    let expired = vtab.lookup().get_expired_partitions(db, now_epoch)?;

    for (partition_value, partition_table_name) in &expired {
        db.execute(&format!("DROP TABLE {}", partition_table_name), ())?;
        vtab.lookup().delete_partition(db, *partition_value)?;
        vtab.stats().delete_partition(db, partition_table_name)?;
    }

    Ok(expired.len() as i64)
}
