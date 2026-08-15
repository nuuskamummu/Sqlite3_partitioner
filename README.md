<p align="center">
  <img src="https://img.shields.io/badge/Rust-000000.svg?style=default&logo=Rust&logoColor=white" alt="Rust">
  <img src="https://img.shields.io/badge/SQLite3-003B57.svg?style=flat&logo=SQLite&logoColor=white" alt="SQLite3">
</p>

# sqlite3-partitioner

A SQLite extension for time-series partitioning. It exposes a
virtual table that transparently splits your data into separate physical tables —
one per time window — so inserts stay fast, retention is a cheap `DROP TABLE`,
and time-ranged queries only ever touch the partitions that matter.

Think of it as declarative partitioning for SQLite: you get partition pruning,
per-partition indexes, fast counts, and expiry-based cleanup, while your
application keeps talking to one ordinary-looking table.

The headline feature is **declarative retention**: declare a `lifetime` at
table creation and every partition knows when it expires. One call to
`partitioner_cleanup('your_table')` drops all expired partitions — you decide
when, nothing runs on a schedule. No ranged `DELETE`s, no index churn, no
fragmentation. Old data simply stops existing.

## Performance

Partitioned vs. a plain table with an equivalent schema and indexes
(macOS, release build; full methodology and raw logs in
[benchmark-results/REPORT.md](benchmark-results/REPORT.md)):

| Workload | Partitioned | Plain table | Result |
|:---------|------------:|------------:|:-------|
| Insert 10.08M rows, on-disk | 117.8 s | 4044.1 s | **34× faster** |
| Insert 1.44M rows (12 cols, 5 indexes), on-disk | 34.0 s | 793.7 s | **23× faster** |
| Insert 10.08M rows, in-memory | 22.4 s | 29.1 s | 1.3× faster |
| Remove 60 000 old rows | 3.3 ms (`DROP TABLE`) | 32.2 ms (ranged `DELETE`) | **~10× faster** |
| `partitioner_count_between` over 10.08M rows | ~0.4 ms | — | **~8× faster** than plain count |

Retention is roughly constant in partition size while a ranged `DELETE` is
O(rows), so the gap grows with partition size. Ad-hoc queries that don't
constrain the partition column run at parity to somewhat slower — see
[Known limitations](#known-limitations) for the honest trade-offs.

## Why partition?

A monolithic time-series table degrades as it grows: every insert maintains
ever-larger b-trees (one per index), and deleting old data is an
O(rows × indexes) operation that fragments the file. With partitioning:

- **Inserts stay flat.** Each partition has its own small b-trees; only the
  current partition is hot.
- **Retention is O(1).** Dropping an expired partition is milliseconds,
  regardless of row count — versus a ranged `DELETE` that scales linearly and
  churns every index.
- **Range queries prune.** A `WHERE ts >= ... AND ts < ...` predicate skips
  partitions outside the window entirely, and `ORDER BY ts` is served from
  partition order with no external sort.

## Installation

Download the pre-built extension from
[Releases](https://github.com/nuuskamummu/Sqlite3_partitioner/releases):

- `partitioner-aarch64-apple-darwin.dylib`
- `partitioner-x86_64-unknown-linux-gnu.so`
- `partitioner-x86_64-pc-windows-msvc.dll`

Load it in the SQLite CLI:

```sql
-- macOS
.load PATH/partitioner-aarch64-apple-darwin

-- Linux
.load PATH/partitioner-x86_64-unknown-linux-gnu

-- Windows
.load PATH/partitioner-x86_64-pc-windows-msvc
```

Or from your application via `sqlite3_load_extension()`.

## Usage

Create a partitioned table:

```sql
CREATE VIRTUAL TABLE events USING partitioner(
    1 hour,                              -- partition interval
    lifetime 31 day,                     -- optional: partitions expire 31 days after their window
    ts timestamp partition_column,       -- the partition column
    device_id text,
    value real
);
```

The `lifetime` clause is optional but recommended for time-series data: each
partition records its expiry, and `partitioner_cleanup` (below) drops expired
partitions on demand. Accepted intervals are `[integer] hour` and
`[integer] day`. From here on, `events` behaves like a normal table — insert,
select, update, delete:

```sql
INSERT INTO events (ts, device_id, value) VALUES ('2026-08-15 13:30', 'pump-7', 41.2);
```

Rows are routed to the right partition automatically; partitions are created
on demand. Inserts are buffered and flushed as multi-row batches (batch size
compile-time configurable via `PARTITIONER_INSERT_BATCH_SIZE`, default 1000).

### Indexing

The partition column is indexed automatically on every partition. To add
secondary indexes, create them on the `<name>_template` table — every partition
created afterwards inherits them:

```sql
CREATE INDEX events_device_idx ON events_template(device_id);
```

(Partitions created before the index do not get it retroactively.)

### Fast partition counts

`partitioner_count_between(table, start, end)` answers "how many rows in this
time range" from per-partition statistics, without scanning rows:

```sql
SELECT partitioner_count_between('events', '2026-08-01', '2026-08-15');
```

Sub-millisecond at any table size, as long as the bounds align to partition
boundaries.

### Retention and cleanup

Give the table a `lifetime` and each partition records when it expires:

```sql
CREATE VIRTUAL TABLE events USING partitioner(
    1 day,
    lifetime 31 day,
    ts timestamp partition_column,
    payload text
);
```

Then prune expired partitions explicitly — you control when the write lock is
taken:

```sql
SELECT partitioner_cleanup('events');  -- drops expired partitions, returns count dropped
```

Run it from cron, a timer, or your application's maintenance window. Nothing
runs on a schedule by itself; the extension only acts when called.

## How it works

For each partitioned table the extension maintains four shadow tables:

- `<name>_template` — the schema blueprint; new partitions are copied from it
  (including indexes).
- `<name>_lookup` — maps partition start epoch → partition table name, plus the
  expiry timestamp.
- `<name>_root` — table configuration (partition column, interval, lifetime).
- `<name>_stats` — per-partition row counts backing `partitioner_count_between`
  and the query planner's cost estimates.

At query time the virtual table's planner hook translates predicates on the
partition column into a partition range, scans only those partitions, and tells
SQLite which constraints it has already enforced. Partitions are disjoint,
ordered ranges of the partition column, so `ORDER BY <partition column>` is
answered by scanning partitions in order — no temp b-tree.

More detail at https://nuuskamummu.github.io/Sqlite3_partitioner/ and in
[benchmark-results/REPORT.md](benchmark-results/REPORT.md).

## Building from source

```bash
cargo build --release
# target/release/libpartitioner.{dylib,so} or partitioner.dll
```

Tests: `cargo test`. Benchmarks (ignored by default):
`cargo test --release -- --ignored --nocapture`; see
[benchmark-results/run_campaign.sh](benchmark-results/run_campaign.sh) for
workload and storage-mode knobs.

## Known limitations

- Experimental; not recommended for production without further testing.
- The datetime parser may not handle all formats (ISO `YYYY-MM-DD [HH:MM[:SS]]`
  is the safe path).
- Ad-hoc queries that don't constrain the partition column can be slower than a
  plain table (virtual table dispatch overhead) — partitioning pays off on
  ingest, retention, and time-ranged access patterns.
- Shadow tables are visible in the schema; altering them directly is undefined
  behavior.

## License

Apache 2.0. See [LICENSE](LICENSE).

## Acknowledgments

- https://github.com/CGamesPlay/sqlite3_ext
