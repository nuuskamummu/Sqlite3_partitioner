use std::ops::{Index, IndexMut};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime};
use rusqlite::Connection as RusqConn;
use sqlite3_ext::query::ToParam;
use sqlite3_ext::{Connection, FallibleIteratorMut, FromValue, TransactionType};

use crate::vtab_interface::init;

const DEFAULT_BENCH_ROWS_PER_MINUTE: usize = 1_000;
const DEFAULT_BENCH_MINUTES: usize = 360;
const QUERY_FILTER_CATEGORY: &str = "category-3";

#[derive(Clone, Copy, Debug)]
struct BenchmarkWorkload {
    rows_per_minute: usize,
    minutes: usize,
}

impl BenchmarkWorkload {
    fn from_env() -> Self {
        Self {
            rows_per_minute: read_env_usize(
                "PARTITIONER_BENCH_ROWS_PER_MINUTE",
                DEFAULT_BENCH_ROWS_PER_MINUTE,
            ),
            minutes: read_env_usize("PARTITIONER_BENCH_MINUTES", DEFAULT_BENCH_MINUTES),
        }
    }

    fn total_rows(&self) -> usize {
        self.rows_per_minute * self.minutes
    }

    fn label(&self) -> String {
        format!(
            "{} rows/minute for {} minutes ({} total rows)",
            self.rows_per_minute,
            self.minutes,
            self.total_rows()
        )
    }

    fn timestamp_for_row(&self, i: usize) -> String {
        let minute_index = i / self.rows_per_minute;
        let timestamp = benchmark_start_timestamp() + ChronoDuration::minutes(minute_index as i64);
        timestamp.format("%Y-%m-%d %H:%M").to_string()
    }

    fn payload_for_row(&self, i: usize) -> String {
        let minute_index = i / self.rows_per_minute;
        format!("category-{}", minute_index % 17)
    }

    fn query_window(&self) -> (String, String) {
        let start_minute = self.window_start_minute(120);
        let start = benchmark_start_timestamp() + ChronoDuration::minutes(start_minute as i64);
        let end = start + ChronoDuration::hours(2);
        (
            start.format("%Y-%m-%d %H:%M").to_string(),
            end.format("%Y-%m-%d %H:%M").to_string(),
        )
    }

    fn delete_window(&self) -> (String, String) {
        // Align to a partition (hour) boundary so the window matches exactly one partition.
        let start_minute = self.window_start_minute(60) / 60 * 60;
        let start = benchmark_start_timestamp() + ChronoDuration::minutes(start_minute as i64);
        let end = start + ChronoDuration::hours(1);
        (
            start.format("%Y-%m-%d %H:%M").to_string(),
            end.format("%Y-%m-%d %H:%M").to_string(),
        )
    }

    fn window_start_minute(&self, window_minutes: usize) -> usize {
        let usable_minutes = self.minutes.saturating_sub(window_minutes);
        usable_minutes / 2
    }
}

fn benchmark_start_timestamp() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2024, 1, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .expect("valid fixed benchmark start timestamp")
}

fn read_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn setup_db(db: &Connection) -> sqlite3_ext::Result<()> {
    init(db)?;
    // The partitioner automatically indexes the partition column on the template
    // (inherited by every partition); the plain table gets the same index manually.
    db.execute(
        "CREATE VIRTUAL TABLE partitioned USING partitioner(1 hour, col1 timestamp partition_column, col2 text)",
        (),
    )?;
    db.execute("CREATE TABLE plain (col1 TEXT, col2 TEXT)", ())?;
    db.execute("CREATE INDEX plain_col1_idx ON plain(col1)", ())?;
    Ok(())
}

fn setup_partitioned_db(db: &Connection) -> sqlite3_ext::Result<()> {
    init(db)?;
    db.execute(
        "CREATE VIRTUAL TABLE partitioned USING partitioner(1 hour, col1 timestamp partition_column, col2 text)",
        (),
    )?;
    Ok(())
}

fn setup_plain_db(db: &Connection) -> sqlite3_ext::Result<()> {
    db.execute("CREATE TABLE plain (col1 TEXT, col2 TEXT)", ())?;
    db.execute("CREATE INDEX plain_col1_idx ON plain(col1)", ())?;
    Ok(())
}

fn bench_row_parts(workload: BenchmarkWorkload, i: usize) -> (String, String) {
    (workload.timestamp_for_row(i), workload.payload_for_row(i))
}

fn load_rows(db: &Connection, workload: BenchmarkWorkload) -> sqlite3_ext::Result<()> {
    let rows = workload.total_rows();
    let txn = db.transaction(TransactionType::Immediate)?;
    for i in 0..rows {
        let (ts, payload) = bench_row_parts(workload, i);
        txn.execute(
            "INSERT INTO partitioned VALUES (?, ?)",
            |stmt: &mut sqlite3_ext::query::Statement| {
                ts.as_str().bind_param(stmt, 1)?;
                payload.as_str().bind_param(stmt, 2)?;
                Ok(())
            },
        )?;
        txn.execute(
            "INSERT INTO plain VALUES (?, ?)",
            |stmt: &mut sqlite3_ext::query::Statement| {
                ts.as_str().bind_param(stmt, 1)?;
                payload.as_str().bind_param(stmt, 2)?;
                Ok(())
            },
        )?;
    }
    txn.commit()?;
    Ok(())
}

fn load_rows_partitioned_only(
    db: &Connection,
    workload: BenchmarkWorkload,
) -> sqlite3_ext::Result<()> {
    let rows = workload.total_rows();
    let txn = db.transaction(TransactionType::Immediate)?;
    for i in 0..rows {
        let (ts, payload) = bench_row_parts(workload, i);
        txn.execute(
            "INSERT INTO partitioned VALUES (?, ?)",
            |stmt: &mut sqlite3_ext::query::Statement| {
                ts.as_str().bind_param(stmt, 1)?;
                payload.as_str().bind_param(stmt, 2)?;
                Ok(())
            },
        )?;
    }
    txn.commit()?;
    Ok(())
}

fn load_rows_plain_only(db: &Connection, workload: BenchmarkWorkload) -> sqlite3_ext::Result<()> {
    let rows = workload.total_rows();
    let txn = db.transaction(TransactionType::Immediate)?;
    for i in 0..rows {
        let (ts, payload) = bench_row_parts(workload, i);
        txn.execute(
            "INSERT INTO plain VALUES (?, ?)",
            |stmt: &mut sqlite3_ext::query::Statement| {
                ts.as_str().bind_param(stmt, 1)?;
                payload.as_str().bind_param(stmt, 2)?;
                Ok(())
            },
        )?;
    }
    txn.commit()?;
    Ok(())
}

fn timed_count(db: &Connection, sql: &str) -> sqlite3_ext::Result<(i64, Duration)> {
    let start = Instant::now();
    let count = db.query_row(sql, (), |row| Ok(row.index(0).get_i64()))?;
    Ok((count, start.elapsed()))
}

fn timed_execute(db: &Connection, sql: &str) -> sqlite3_ext::Result<Duration> {
    let start = Instant::now();
    db.execute(sql, ())?;
    Ok(start.elapsed())
}

fn count_partition_window(
    db: &Connection,
    delete_window_start: &str,
    delete_window_end: &str,
) -> sqlite3_ext::Result<i64> {
    db.query_row(
        &format!(
            "SELECT count(*) FROM partitioned WHERE col1 >= '{delete_window_start}' AND col1 < '{delete_window_end}'"
        ),
        (),
        |row| Ok(row.index(0).get_i64()),
    )
}

fn count_plain_window(
    db: &Connection,
    delete_window_start: &str,
    delete_window_end: &str,
) -> sqlite3_ext::Result<i64> {
    db.query_row(
        &format!(
            "SELECT count(*) FROM plain WHERE col1 >= '{delete_window_start}' AND col1 < '{delete_window_end}'"
        ),
        (),
        |row| Ok(row.index(0).get_i64()),
    )
}

fn manual_drop_partition_window(
    db: &Connection,
    partition_start: &str,
) -> sqlite3_ext::Result<Duration> {
    let partition_name = db.query_row(
        "SELECT partition_table FROM partitioned_lookup WHERE partition_value = strftime('%s', ?)",
        [partition_start],
        |row| Ok(row.index_mut(0).get_str()?.to_string()),
    )?;

    let start = Instant::now();
    db.execute(&format!("DROP TABLE {}", partition_name), ())?;
    db.execute(
        "DELETE FROM partitioned_lookup WHERE partition_table = ?",
        [partition_name.as_str()],
    )?;
    db.execute(
        "DELETE FROM partitioned_stats WHERE partition_table = ?",
        [partition_name.as_str()],
    )?;
    Ok(start.elapsed())
}

fn temp_benchmark_db_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "sqlite3_partitioner_{name}_{}.db",
        std::process::id()
    ));
    path
}

/// Benchmark storage backend: `PARTITIONER_BENCH_STORAGE=disk` uses a temp file,
/// anything else (default) is in-memory.
fn bench_storage_label() -> &'static str {
    match std::env::var("PARTITIONER_BENCH_STORAGE").as_deref() {
        Ok("disk") => "disk",
        _ => "memory",
    }
}

fn bench_rusqlite_conn(name: &str) -> RusqConn {
    match std::env::var("PARTITIONER_BENCH_STORAGE").as_deref() {
        Ok("disk") => {
            let path = temp_benchmark_db_path(name);
            let _ = std::fs::remove_file(&path);
            RusqConn::open(path).unwrap()
        }
        _ => RusqConn::open_in_memory().unwrap(),
    }
}

#[test]
#[ignore = "manual benchmark; run with cargo test benchmark_partitioned_vs_plain_unindexed_filter -- --ignored --nocapture"]
fn benchmark_partitioned_vs_plain_unindexed_filter() -> sqlite3_ext::Result<()> {
    let workload = BenchmarkWorkload::from_env();
    let (query_window_start, query_window_end) = workload.query_window();
    let rusq_conn = bench_rusqlite_conn("bench_unindexed");
    let db = Connection::from_rusqlite(&rusq_conn);
    setup_db(db)?;
    load_rows(db, workload)?;

    let query = format!(
        "SELECT count(*) FROM {{}} WHERE col1 >= '{}' AND col1 < '{}' AND col2 = '{}'",
        query_window_start, query_window_end, QUERY_FILTER_CATEGORY
    );
    let (partitioned_count, partitioned_duration) =
        timed_count(db, &query.replace("{}", "partitioned"))?;
    let (plain_count, plain_duration) = timed_count(db, &query.replace("{}", "plain"))?;

    assert_eq!(partitioned_count, plain_count);

    println!(
        "unindexed-filter benchmark on {} [{}]",
        workload.label(),
        bench_storage_label()
    );
    println!(
        "partitioned count={} duration={:?}",
        partitioned_count, partitioned_duration
    );
    println!("plain count={} duration={:?}", plain_count, plain_duration);

    Ok(())
}

#[test]
#[ignore = "manual benchmark; run with cargo test benchmark_latest_rows_order_by -- --ignored --nocapture"]
fn benchmark_latest_rows_order_by() -> sqlite3_ext::Result<()> {
    let workload = BenchmarkWorkload::from_env();
    let rusq_conn = bench_rusqlite_conn("bench_latest_rows");
    let db = Connection::from_rusqlite(&rusq_conn);
    setup_db(db)?;
    load_rows(db, workload)?;

    let timed_latest = |table: &str| -> sqlite3_ext::Result<(Vec<String>, Duration)> {
        let sql = format!("SELECT col1 FROM {} ORDER BY col1 DESC LIMIT 1000", table);
        let start = Instant::now();
        let mut stmt = db.prepare(&sql)?;
        let rows = stmt.query(())?;
        let mut values = Vec::new();
        while let Ok(Some(row)) = rows.next() {
            values.push(row.index_mut(0).get_str()?.to_string());
        }
        Ok((values, start.elapsed()))
    };

    let (partitioned_values, partitioned_duration) = timed_latest("partitioned")?;
    let (plain_values, plain_duration) = timed_latest("plain")?;
    assert_eq!(partitioned_values, plain_values);
    assert_eq!(partitioned_values.len(), 1000);

    println!(
        "latest-rows benchmark on {} [{}]",
        workload.label(),
        bench_storage_label()
    );
    println!(
        "partitioned ORDER BY col1 DESC LIMIT 1000 duration={:?}",
        partitioned_duration
    );
    println!(
        "plain ORDER BY col1 DESC LIMIT 1000 duration={:?}",
        plain_duration
    );

    Ok(())
}

#[test]
#[ignore = "manual benchmark; run with cargo test benchmark_insert_partitioned_vs_plain -- --ignored --nocapture"]
fn benchmark_insert_partitioned_vs_plain() -> sqlite3_ext::Result<()> {
    let workload = BenchmarkWorkload::from_env();
    let partitioned_raw = bench_rusqlite_conn("bench_insert_partitioned");
    let partitioned_db = Connection::from_rusqlite(&partitioned_raw);
    setup_partitioned_db(partitioned_db)?;

    let plain_raw = bench_rusqlite_conn("bench_insert_plain");
    let plain_db = Connection::from_rusqlite(&plain_raw);
    setup_plain_db(plain_db)?;

    let partitioned_start = Instant::now();
    for i in 0..workload.total_rows() {
        let (ts, payload) = bench_row_parts(workload, i);
        partitioned_db.execute(
            "INSERT INTO partitioned VALUES (?, ?)",
            |stmt: &mut sqlite3_ext::query::Statement| {
                ts.as_str().bind_param(stmt, 1)?;
                payload.as_str().bind_param(stmt, 2)?;
                Ok(())
            },
        )?;
    }
    let partitioned_duration = partitioned_start.elapsed();

    let plain_start = Instant::now();
    for i in 0..workload.total_rows() {
        let (ts, payload) = bench_row_parts(workload, i);
        plain_db.execute(
            "INSERT INTO plain VALUES (?, ?)",
            |stmt: &mut sqlite3_ext::query::Statement| {
                ts.as_str().bind_param(stmt, 1)?;
                payload.as_str().bind_param(stmt, 2)?;
                Ok(())
            },
        )?;
    }
    let plain_duration = plain_start.elapsed();

    let partitioned_count =
        partitioned_db.query_row("SELECT count(*) FROM partitioned", (), |row| {
            Ok(row.index(0).get_i64())
        })?;
    let plain_count = plain_db.query_row("SELECT count(*) FROM plain", (), |row| {
        Ok(row.index(0).get_i64())
    })?;
    assert_eq!(partitioned_count, plain_count);

    println!(
        "insert benchmark on {} [{}]",
        workload.label(),
        bench_storage_label()
    );
    println!("partitioned insert duration={:?}", partitioned_duration);
    println!("plain insert duration={:?}", plain_duration);

    Ok(())
}

#[test]
#[ignore = "manual benchmark; run with cargo test benchmark_partition_drop_vs_plain_delete -- --ignored --nocapture"]
fn benchmark_partition_drop_vs_plain_delete() -> sqlite3_ext::Result<()> {
    let workload = BenchmarkWorkload::from_env();
    let (delete_window_start, delete_window_end) = workload.delete_window();
    let partitioned_path = temp_benchmark_db_path("partition_delete_bench");
    let plain_path = temp_benchmark_db_path("plain_delete_bench");
    let _ = std::fs::remove_file(&partitioned_path);
    let _ = std::fs::remove_file(&plain_path);

    {
        let raw = RusqConn::open(&partitioned_path).unwrap();
        let db = Connection::from_rusqlite(&raw);
        setup_partitioned_db(db)?;
        load_rows_partitioned_only(db, workload)?;
    }

    {
        let raw = RusqConn::open(&plain_path).unwrap();
        let db = Connection::from_rusqlite(&raw);
        setup_plain_db(db)?;
        load_rows_plain_only(db, workload)?;
    }

    let before_partitioned = {
        let raw = RusqConn::open(&partitioned_path).unwrap();
        let db = Connection::from_rusqlite(&raw);
        init(db)?;
        count_partition_window(db, &delete_window_start, &delete_window_end)?
    };
    let before_plain = {
        let raw = RusqConn::open(&plain_path).unwrap();
        let db = Connection::from_rusqlite(&raw);
        count_plain_window(db, &delete_window_start, &delete_window_end)?
    };
    assert_eq!(before_partitioned, before_plain);

    let partitioned_duration = {
        let raw = RusqConn::open(&partitioned_path).unwrap();
        let db = Connection::from_rusqlite(&raw);
        init(db)?;
        manual_drop_partition_window(db, &delete_window_start)?
    };
    let plain_duration = {
        let raw = RusqConn::open(&plain_path).unwrap();
        let db = Connection::from_rusqlite(&raw);
        timed_execute(
            db,
            &format!(
                "DELETE FROM plain WHERE col1 >= '{}' AND col1 < '{}'",
                delete_window_start, delete_window_end
            ),
        )?
    };

    {
        let raw = RusqConn::open(&plain_path).unwrap();
        let db = Connection::from_rusqlite(&raw);
        assert_eq!(
            count_plain_window(db, &delete_window_start, &delete_window_end)?,
            0
        );
    }
    {
        let raw = RusqConn::open(&partitioned_path).unwrap();
        let db = Connection::from_rusqlite(&raw);
        init(db)?;
        let remaining_lookup_rows = db.query_row(
            "SELECT count(*) FROM partitioned_lookup WHERE partition_value = strftime('%s', ?)",
            [delete_window_start.as_str()],
            |row| Ok(row.index(0).get_i64()),
        )?;
        let remaining_stats_rows = db.query_row(
            "SELECT count(*) FROM partitioned_stats WHERE partition_table NOT IN (SELECT partition_table FROM partitioned_lookup)",
            (),
            |row| Ok(row.index(0).get_i64()),
        )?;
        assert_eq!(remaining_lookup_rows, 0);
        assert_eq!(remaining_stats_rows, 0);
    }

    println!(
        "partition-drop-vs-delete benchmark on {} [{}]",
        workload.label(),
        bench_storage_label()
    );
    println!(
        "manual partition drop duration={:?} rows_removed={}",
        partitioned_duration, before_partitioned
    );
    println!(
        "plain ranged delete duration={:?} rows_removed={}",
        plain_duration, before_plain
    );

    let _ = std::fs::remove_file(&partitioned_path);
    let _ = std::fs::remove_file(&plain_path);

    Ok(())
}

#[test]
#[ignore = "manual benchmark; run with cargo test benchmark_partitioned_vs_plain -- --ignored --nocapture"]
fn benchmark_partitioned_vs_plain() -> sqlite3_ext::Result<()> {
    let workload = BenchmarkWorkload::from_env();
    let (query_window_start, query_window_end) = workload.query_window();
    let rusq_conn = bench_rusqlite_conn("bench_partitioned_vs_plain");
    let db = Connection::from_rusqlite(&rusq_conn);
    setup_db(db)?;

    let load_start = Instant::now();
    load_rows(db, workload)?;
    let load_duration = load_start.elapsed();

    let query = format!(
        "SELECT count(*) FROM {{}} WHERE col1 >= '{}' AND col1 < '{}'",
        query_window_start, query_window_end
    );
    let (partitioned_count, partitioned_duration) =
        timed_count(db, &query.replace("{}", "partitioned"))?;
    let (stats_count, stats_duration) = timed_count(
        db,
        &format!(
            "SELECT partitioner_count_between('partitioned', '{}', '{}')",
            query_window_start, query_window_end
        ),
    )?;
    let (plain_count, plain_duration) = timed_count(db, &query.replace("{}", "plain"))?;

    assert_eq!(partitioned_count, plain_count);
    assert_eq!(stats_count, partitioned_count);

    println!("loaded {} in {:?}", workload.label(), load_duration);
    println!(
        "partitioned count={} duration={:?}",
        partitioned_count, partitioned_duration
    );
    println!(
        "partitioner_count_between count={} duration={:?}",
        stats_count, stats_duration
    );
    println!("plain count={} duration={:?}", plain_count, plain_duration);

    Ok(())
}

#[derive(Clone, Debug)]
struct RealisticRow {
    ts: String,
    device_id: String,
    status: &'static str,
    region: &'static str,
    category: String,
    subcategory: String,
    value_int: i64,
    value_real: f64,
    counter: i64,
    measurement: f64,
    flags: i64,
    payload: String,
}

const REALISTIC_STATUSES: &[&str] = &["ok", "warn", "err", "unknown"];
const REALISTIC_REGIONS: &[&str] = &["us-east", "us-west", "eu-central", "ap-south"];
const REALISTIC_DEVICE_COUNT: usize = 1_000;
const REALISTIC_CATEGORY_COUNT: usize = 50;
const REALISTIC_SUBCATEGORY_COUNT: usize = 200;
const REALISTIC_FLAGS_COUNT: i64 = 8;

fn bench_realistic_row(workload: BenchmarkWorkload, i: usize) -> RealisticRow {
    RealisticRow {
        ts: workload.timestamp_for_row(i),
        device_id: format!("dev_{:04}", i % REALISTIC_DEVICE_COUNT),
        status: REALISTIC_STATUSES[i % REALISTIC_STATUSES.len()],
        region: REALISTIC_REGIONS[i % REALISTIC_REGIONS.len()],
        category: format!("cat_{:02}", i % REALISTIC_CATEGORY_COUNT),
        subcategory: format!("sub_{:03}", i % REALISTIC_SUBCATEGORY_COUNT),
        value_int: i as i64,
        value_real: ((i % 1_000_000) as f64) / 1_000.0,
        counter: ((i as i64) * 7) % 1_000_000_000,
        measurement: (((i as i64) * 13) % 1_000_000) as f64 / 100.0,
        flags: (i as i64) % REALISTIC_FLAGS_COUNT,
        payload: format!("payload_{:06}_{:04}", i % 10_000, i % 1_000),
    }
}

fn setup_realistic_partitioned_db(db: &Connection) -> sqlite3_ext::Result<()> {
    init(db)?;
    db.execute(
        "CREATE VIRTUAL TABLE partitioned USING partitioner(
            1 hour,
            ts timestamp partition_column,
            device_id text,
            status text,
            region text,
            category text,
            subcategory text,
            value_int integer,
            value_real float,
            counter integer,
            measurement float,
            flags integer,
            payload text
        )",
        (),
    )?;
    db.execute(
        "CREATE INDEX partitioned_device_idx ON partitioned_template(device_id)",
        (),
    )?;
    db.execute(
        "CREATE INDEX partitioned_status_region_idx ON partitioned_template(status, region)",
        (),
    )?;
    db.execute(
        "CREATE INDEX partitioned_category_device_idx ON partitioned_template(category, device_id)",
        (),
    )?;
    db.execute(
        "CREATE INDEX partitioned_value_idx ON partitioned_template(value_int, value_real)",
        (),
    )?;
    db.execute(
        "CREATE INDEX partitioned_counter_idx ON partitioned_template(counter)",
        (),
    )?;
    Ok(())
}

fn setup_realistic_plain_db(db: &Connection) -> sqlite3_ext::Result<()> {
    db.execute(
        "CREATE TABLE plain (
            ts TEXT,
            device_id TEXT,
            status TEXT,
            region TEXT,
            category TEXT,
            subcategory TEXT,
            value_int INTEGER,
            value_real REAL,
            counter INTEGER,
            measurement REAL,
            flags INTEGER,
            payload TEXT
        )",
        (),
    )?;
    db.execute("CREATE INDEX plain_ts_idx ON plain(ts)", ())?;
    db.execute("CREATE INDEX plain_device_idx ON plain(device_id)", ())?;
    db.execute(
        "CREATE INDEX plain_status_region_idx ON plain(status, region)",
        (),
    )?;
    db.execute(
        "CREATE INDEX plain_category_device_idx ON plain(category, device_id)",
        (),
    )?;
    db.execute(
        "CREATE INDEX plain_value_idx ON plain(value_int, value_real)",
        (),
    )?;
    db.execute("CREATE INDEX plain_counter_idx ON plain(counter)", ())?;
    Ok(())
}

fn realistic_query_window(workload: BenchmarkWorkload, width_minutes: usize) -> (String, String) {
    let start_minute = workload.window_start_minute(width_minutes);
    let start = benchmark_start_timestamp() + ChronoDuration::minutes(start_minute as i64);
    let end = start + ChronoDuration::minutes(width_minutes as i64);
    (
        start.format("%Y-%m-%d %H:%M").to_string(),
        end.format("%Y-%m-%d %H:%M").to_string(),
    )
}

#[test]
#[ignore = "manual benchmark; run with cargo test benchmark_realistic_wide_table -- --ignored --nocapture"]
fn benchmark_realistic_wide_table() -> sqlite3_ext::Result<()> {
    let workload = BenchmarkWorkload::from_env();
    let (two_hour_start, two_hour_end) = workload.query_window();
    let (day_start, day_end) = realistic_query_window(workload, 24 * 60);
    let (week_start, week_end) = realistic_query_window(workload, 7 * 24 * 60);

    let partitioned_raw = bench_rusqlite_conn("realistic_partitioned");
    let partitioned_db = Connection::from_rusqlite(&partitioned_raw);
    setup_realistic_partitioned_db(partitioned_db)?;

    let plain_raw = bench_rusqlite_conn("realistic_plain");
    let plain_db = Connection::from_rusqlite(&plain_raw);
    setup_realistic_plain_db(plain_db)?;

    let partitioned_start = Instant::now();
    for i in 0..workload.total_rows() {
        let row = bench_realistic_row(workload, i);
        partitioned_db.execute(
            "INSERT INTO partitioned VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            |stmt: &mut sqlite3_ext::query::Statement| {
                row.ts.as_str().bind_param(stmt, 1)?;
                row.device_id.as_str().bind_param(stmt, 2)?;
                row.status.bind_param(stmt, 3)?;
                row.region.bind_param(stmt, 4)?;
                row.category.as_str().bind_param(stmt, 5)?;
                row.subcategory.as_str().bind_param(stmt, 6)?;
                row.value_int.bind_param(stmt, 7)?;
                row.value_real.bind_param(stmt, 8)?;
                row.counter.bind_param(stmt, 9)?;
                row.measurement.bind_param(stmt, 10)?;
                row.flags.bind_param(stmt, 11)?;
                row.payload.as_str().bind_param(stmt, 12)?;
                Ok(())
            },
        )?;
    }
    let partitioned_duration = partitioned_start.elapsed();

    let plain_start = Instant::now();
    for i in 0..workload.total_rows() {
        let row = bench_realistic_row(workload, i);
        plain_db.execute(
            "INSERT INTO plain VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            |stmt: &mut sqlite3_ext::query::Statement| {
                row.ts.as_str().bind_param(stmt, 1)?;
                row.device_id.as_str().bind_param(stmt, 2)?;
                row.status.bind_param(stmt, 3)?;
                row.region.bind_param(stmt, 4)?;
                row.category.as_str().bind_param(stmt, 5)?;
                row.subcategory.as_str().bind_param(stmt, 6)?;
                row.value_int.bind_param(stmt, 7)?;
                row.value_real.bind_param(stmt, 8)?;
                row.counter.bind_param(stmt, 9)?;
                row.measurement.bind_param(stmt, 10)?;
                row.flags.bind_param(stmt, 11)?;
                row.payload.as_str().bind_param(stmt, 12)?;
                Ok(())
            },
        )?;
    }
    let plain_duration = plain_start.elapsed();

    let partitioned_count =
        partitioned_db.query_row("SELECT count(*) FROM partitioned", (), |row| {
            Ok(row.index(0).get_i64())
        })?;
    let plain_count = plain_db.query_row("SELECT count(*) FROM plain", (), |row| {
        Ok(row.index(0).get_i64())
    })?;
    assert_eq!(partitioned_count, plain_count);

    let sample_device = "dev_0001";
    let sample_status = REALISTIC_STATUSES[0];
    let sample_region = REALISTIC_REGIONS[0];

    let (two_hour_partitioned_count, two_hour_partitioned_duration) = timed_count(
        partitioned_db,
        &format!(
            "SELECT count(*) FROM partitioned WHERE ts >= '{}' AND ts < '{}'",
            two_hour_start, two_hour_end
        ),
    )?;
    let (two_hour_plain_count, two_hour_plain_duration) = timed_count(
        plain_db,
        &format!(
            "SELECT count(*) FROM plain WHERE ts >= '{}' AND ts < '{}'",
            two_hour_start, two_hour_end
        ),
    )?;

    let (status_region_partitioned_count, status_region_partitioned_duration) = timed_count(
        partitioned_db,
        &format!(
            "SELECT count(*) FROM partitioned WHERE ts >= '{}' AND ts < '{}' AND status = '{}' AND region = '{}'",
            day_start, day_end, sample_status, sample_region
        ),
    )?;
    let (status_region_plain_count, status_region_plain_duration) = timed_count(
        plain_db,
        &format!(
            "SELECT count(*) FROM plain WHERE ts >= '{}' AND ts < '{}' AND status = '{}' AND region = '{}'",
            day_start, day_end, sample_status, sample_region
        ),
    )?;

    let (device_partitioned_count, device_partitioned_duration) = timed_count(
        partitioned_db,
        &format!(
            "SELECT count(*) FROM partitioned WHERE ts >= '{}' AND ts < '{}' AND device_id = '{}'",
            day_start, day_end, sample_device
        ),
    )?;
    let (device_plain_count, device_plain_duration) = timed_count(
        plain_db,
        &format!(
            "SELECT count(*) FROM plain WHERE ts >= '{}' AND ts < '{}' AND device_id = '{}'",
            day_start, day_end, sample_device
        ),
    )?;

    let (avg_partitioned_count, avg_partitioned_duration) = timed_count(
        partitioned_db,
        &format!(
            "SELECT count(*) FROM (SELECT category, avg(value_real) FROM partitioned WHERE ts >= '{}' AND ts < '{}' GROUP BY category)",
            week_start, week_end
        ),
    )?;
    let (avg_plain_count, avg_plain_duration) = timed_count(
        plain_db,
        &format!(
            "SELECT count(*) FROM (SELECT category, avg(value_real) FROM plain WHERE ts >= '{}' AND ts < '{}' GROUP BY category)",
            week_start, week_end
        ),
    )?;

    println!(
        "realistic wide-table benchmark on {} [{}]",
        workload.label(),
        bench_storage_label()
    );
    println!(
        "partitioned insert batch size={}",
        crate::shadow_tables::interface::INSERT_BATCH_SIZE
    );
    println!("partitioned insert duration={:?}", partitioned_duration);
    println!("plain insert duration={:?}", plain_duration);
    println!(
        "2-hour ts range: partitioned count={} duration={:?} | plain count={} duration={:?}",
        two_hour_partitioned_count,
        two_hour_partitioned_duration,
        two_hour_plain_count,
        two_hour_plain_duration
    );
    println!(
        "1-day status+region: partitioned count={} duration={:?} | plain count={} duration={:?}",
        status_region_partitioned_count,
        status_region_partitioned_duration,
        status_region_plain_count,
        status_region_plain_duration
    );
    println!(
        "1-day device_id: partitioned count={} duration={:?} | plain count={} duration={:?}",
        device_partitioned_count,
        device_partitioned_duration,
        device_plain_count,
        device_plain_duration
    );
    println!(
        "1-week avg(value_real) by category: partitioned groups={} duration={:?} | plain groups={} duration={:?}",
        avg_partitioned_count, avg_partitioned_duration,
        avg_plain_count, avg_plain_duration
    );

    let _ = std::fs::remove_file(temp_benchmark_db_path("realistic_partitioned"));
    let _ = std::fs::remove_file(temp_benchmark_db_path("realistic_plain"));

    Ok(())
}

// --- Vector companion benchmark (feature "vec", requires VEC0_EXTENSION_PATH) ---

#[cfg(feature = "vec")]
mod vec_bench {
    use super::*;

    const DEFAULT_VEC_DIM: usize = 64;

    fn vec_dim() -> usize {
        read_env_usize("PARTITIONER_BENCH_VEC_DIM", DEFAULT_VEC_DIM)
    }

    /// Deterministic pseudo-vector for row `i` as a JSON array text.
    fn vector_for_row(i: usize, dim: usize) -> String {
        let mut out = String::with_capacity(dim * 8);
        out.push('[');
        for d in 0..dim {
            if d > 0 {
                out.push(',');
            }
            let v = ((i * 31 + d * 17) % 1000) as f64 / 1000.0;
            out.push_str(&format!("{:.3}", v));
        }
        out.push(']');
        out
    }

    fn load_vec0(rusq: &RusqConn) -> bool {
        let path = match std::env::var("VEC0_EXTENSION_PATH") {
            Ok(path) => path,
            Err(_) => {
                eprintln!("skipping: VEC0_EXTENSION_PATH not set");
                return false;
            }
        };
        unsafe {
            rusq.load_extension_enable().unwrap();
            rusq.load_extension(&path, None).unwrap();
            rusq.load_extension_disable().unwrap();
        }
        true
    }

    /// Compares a plain table with a manually-synced vec0 index against a
    /// partitioned table with an auto-synced vec companion: ingest, KNN,
    /// time-windowed KNN (post-filter, vec0 0.1.9 rejects aux-column filters
    /// inside KNN), and retention purge.
    #[test]
    #[ignore = "manual benchmark; requires --features vec and VEC0_EXTENSION_PATH"]
    fn benchmark_vec_companion_vs_plain() -> sqlite3_ext::Result<()> {
        let rusq = bench_rusqlite_conn("vec_bench");
        if !load_vec0(&rusq) {
            return Ok(());
        }
        let db = Connection::from_rusqlite(&rusq);
        let workload = BenchmarkWorkload::from_env();
        let dim = vec_dim();
        let rows = workload.total_rows();

        init(db)?;
        db.execute(
            "CREATE VIRTUAL TABLE partitioned USING partitioner(1 hour, col1 timestamp partition_column, col2 text, emb text, companion vec USING vec0(emb float[64]))",
            (),
        )?;
        // The companion clause carries the dim literally; adjust if overridden.
        if dim != DEFAULT_VEC_DIM {
            panic!("PARTITIONER_BENCH_VEC_DIM override not supported by this benchmark's DDL");
        }
        db.execute("CREATE TABLE plain (col1 TEXT, col2 TEXT, emb TEXT)", ())?;
        db.execute("CREATE INDEX plain_col1_idx ON plain(col1)", ())?;
        db.execute(
            "CREATE VIRTUAL TABLE plain_vec USING vec0(emb float[64])",
            (),
        )?;

        // --- Phase 1: ingest (plain side syncs vec0 by hand, row by row) ---
        let plain_insert = {
            let start = Instant::now();
            let txn = db.transaction(TransactionType::Immediate)?;
            for i in 0..rows {
                let (ts, payload) = bench_row_parts(workload, i);
                let vector = vector_for_row(i, dim);
                txn.execute(
                    "INSERT INTO plain VALUES (?, ?, ?)",
                    |stmt: &mut sqlite3_ext::query::Statement| {
                        ts.as_str().bind_param(stmt, 1)?;
                        payload.as_str().bind_param(stmt, 2)?;
                        vector.as_str().bind_param(stmt, 3)?;
                        Ok(())
                    },
                )?;
                txn.execute(
                    "INSERT INTO plain_vec(rowid, emb) VALUES (last_insert_rowid(), ?)",
                    |stmt: &mut sqlite3_ext::query::Statement| {
                        vector.as_str().bind_param(stmt, 1)?;
                        Ok(())
                    },
                )?;
            }
            txn.commit()?;
            start.elapsed()
        };

        let partitioned_insert = {
            let start = Instant::now();
            let txn = db.transaction(TransactionType::Immediate)?;
            for i in 0..rows {
                let (ts, payload) = bench_row_parts(workload, i);
                let vector = vector_for_row(i, dim);
                txn.execute(
                    "INSERT INTO partitioned VALUES (?, ?, ?)",
                    |stmt: &mut sqlite3_ext::query::Statement| {
                        ts.as_str().bind_param(stmt, 1)?;
                        payload.as_str().bind_param(stmt, 2)?;
                        vector.as_str().bind_param(stmt, 3)?;
                        Ok(())
                    },
                )?;
            }
            txn.commit()?;
            start.elapsed()
        };

        let plain_vec_count: i64 =
            db.query_row("SELECT count(*) FROM plain_vec", (), |row| {
                Ok(row.index(0).get_i64())
            })?;
        let partitioned_vec_count: i64 =
            db.query_row("SELECT count(*) FROM partitioned_vec", (), |row| {
                Ok(row.index(0).get_i64())
            })?;

        // --- Phase 2: pure KNN, k=10, full resolution round trip ---
        let query_vector = vector_for_row(rows / 3, dim);

        let plain_knn = {
            let start = Instant::now();
            let mut stmt = db.prepare(
                "SELECT rowid, distance FROM plain_vec WHERE emb MATCH ? AND k = 10 ORDER BY distance",
            )?;
            let rows_iter = stmt.query([query_vector.as_str()])?;
            let mut resolved = 0usize;
            while let Ok(Some(row)) = rows_iter.next() {
                let rowid = row[0].get_i64();
                let _: String = db.query_row(
                    "SELECT col2 FROM plain WHERE rowid = ?",
                    [rowid],
                    |r| Ok(r.index_mut(0).get_str()?.to_owned()),
                )?;
                resolved += 1;
            }
            assert_eq!(resolved, 10);
            start.elapsed()
        };

        let partitioned_knn = {
            let start = Instant::now();
            let mut stmt = db.prepare(
                "SELECT epoch, prowid, distance FROM partitioned_vec WHERE emb MATCH ? AND k = 10 ORDER BY distance",
            )?;
            let rows_iter = stmt.query([query_vector.as_str()])?;
            let mut resolved = 0usize;
            while let Ok(Some(row)) = rows_iter.next() {
                let epoch = row[0].get_i64();
                let prowid = row[1].get_i64();
                let _: String = db.query_row(
                    &format!("SELECT col2 FROM partitioned_{} WHERE rowid = ?", epoch),
                    [prowid],
                    |r| Ok(r.index_mut(0).get_str()?.to_owned()),
                )?;
                resolved += 1;
            }
            assert_eq!(resolved, 10);
            start.elapsed()
        };

        // --- Phase 3: time-windowed KNN (over-fetch 100, post-filter to 10) ---
        let (window_start, window_end) = workload.query_window();
        let window_start_epoch = db.query_row(
            "SELECT strftime('%s', ?)",
            [window_start.as_str()],
            |row| Ok(row.index(0).get_i64()),
        )?;
        let window_end_epoch: i64 = db.query_row(
            "SELECT strftime('%s', ?)",
            [window_end.as_str()],
            |row| Ok(row.index(0).get_i64()),
        )?;

        let plain_window_knn = {
            let start = Instant::now();
            let mut stmt = db.prepare(
                "SELECT rowid FROM plain_vec WHERE emb MATCH ? AND k = 100 ORDER BY distance",
            )?;
            let rows_iter = stmt.query([query_vector.as_str()])?;
            let mut kept = 0usize;
            while let Ok(Some(row)) = rows_iter.next() {
                let in_window: i64 = db.query_row(
                    "SELECT count(*) FROM plain WHERE rowid = ? AND col1 >= ? AND col1 < ?",
                    |stmt: &mut sqlite3_ext::query::Statement| {
                        row[0].get_i64().bind_param(stmt, 1)?;
                        window_start.as_str().bind_param(stmt, 2)?;
                        window_end.as_str().bind_param(stmt, 3)?;
                        Ok(())
                    },
                    |r| Ok(r.index(0).get_i64()),
                )?;
                kept += in_window as usize;
                if kept >= 10 {
                    break;
                }
            }
            start.elapsed()
        };

        let partitioned_window_knn = {
            let start = Instant::now();
            let mut stmt = db.prepare(
                "SELECT epoch FROM partitioned_vec WHERE emb MATCH ? AND k = 100 ORDER BY distance",
            )?;
            let rows_iter = stmt.query([query_vector.as_str()])?;
            let mut kept = 0usize;
            while let Ok(Some(row)) = rows_iter.next() {
                let epoch = row[0].get_i64();
                let partition_start = epoch - epoch.rem_euclid(3600);
                if partition_start >= window_start_epoch && partition_start < window_end_epoch {
                    kept += 1;
                    if kept >= 10 {
                        break;
                    }
                }
            }
            start.elapsed()
        };

        // --- Phase 4: retention purge of one hour ---
        let (delete_start, delete_end) = workload.delete_window();
        let delete_start_epoch: i64 = db.query_row(
            "SELECT strftime('%s', ?)",
            [delete_start.as_str()],
            |row| Ok(row.index(0).get_i64()),
        )?;

        let plain_purge = {
            let start = Instant::now();
            db.execute(
                &format!(
                    "DELETE FROM plain_vec WHERE rowid IN (SELECT rowid FROM plain WHERE col1 >= '{delete_start}' AND col1 < '{delete_end}')"
                ),
                (),
            )?;
            let vec_delete = start.elapsed();
            let start_rows = Instant::now();
            db.execute(
                &format!(
                    "DELETE FROM plain WHERE col1 >= '{delete_start}' AND col1 < '{delete_end}'"
                ),
                (),
            )?;
            let rows_delete = start_rows.elapsed();
            println!(
                "  plain purge breakdown: vec rows delete={:?} plain rows delete={:?}",
                vec_delete, rows_delete
            );
            start.elapsed()
        };

        let partitioned_purge = {
            let partition_name: String = db.query_row(
                "SELECT partition_table FROM partitioned_lookup WHERE partition_value = ?",
                [delete_start_epoch],
                |row| Ok(row.index_mut(0).get_str()?.to_string()),
            )?;
            let start = Instant::now();
            db.execute(
                &format!("DELETE FROM partitioned_vec WHERE rowid IN (SELECT vec_rowid FROM partitioned_vec_keys WHERE epoch = {})", delete_start_epoch),
                (),
            )?;
            let vec_delete = start.elapsed();
            let start_keys = Instant::now();
            db.execute(
                &format!("DELETE FROM partitioned_vec_keys WHERE epoch = {}", delete_start_epoch),
                (),
            )?;
            let keys_delete = start_keys.elapsed();
            let start_drop = Instant::now();
            db.execute(&format!("DROP TABLE {}", partition_name), ())?;
            db.execute(
                "DELETE FROM partitioned_lookup WHERE partition_table = ?",
                [partition_name.as_str()],
            )?;
            db.execute(
                "DELETE FROM partitioned_stats WHERE partition_table = ?",
                [partition_name.as_str()],
            )?;
            let drop_meta = start_drop.elapsed();
            println!(
                "  purge breakdown: vec rows delete={:?} keys delete={:?} drop+meta={:?}",
                vec_delete, keys_delete, drop_meta
            );
            start.elapsed()
        };

        println!("storage={} dim={} workload={}", bench_storage_label(), dim, workload.label());
        println!(
            "ingest: partitioned+companion={:?} | plain+manual vec sync={:?}",
            partitioned_insert, plain_insert
        );
        println!(
            "vec row counts: partitioned_vec={} plain_vec={}",
            partitioned_vec_count, plain_vec_count
        );
        println!(
            "knn k=10 (with resolution): partitioned={:?} | plain={:?}",
            partitioned_knn, plain_knn
        );
        println!(
            "windowed knn k=10 (over-fetch 100, post-filter): partitioned={:?} | plain={:?}",
            partitioned_window_knn, plain_window_knn
        );
        println!(
            "retention purge (1 hour of rows + vectors): partitioned={:?} | plain={:?}",
            partitioned_purge, plain_purge
        );

        let _ = std::fs::remove_file(temp_benchmark_db_path("vec_bench"));
        Ok(())
    }
}
