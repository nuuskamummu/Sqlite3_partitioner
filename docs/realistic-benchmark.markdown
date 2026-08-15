---
layout: default
title: Realistic Benchmark
permalink: /realistic-benchmark
nav_order: 5
---

# Benchmarks

The benchmark suite compares the partitioned virtual table against a plain
SQLite table with an equivalent schema and indexes. Headline numbers below;
full methodology and raw logs live in
[benchmark-results/REPORT.md](https://github.com/nuuskamummu/Sqlite3_partitioner/blob/master/benchmark-results/REPORT.md).

## Workloads

- **Week scale, narrow table**: 1000 rows/minute for one week — 10 080 000
  rows, `(ts timestamp, value text)`, hourly partitions (168 partitions).
  Run both in-memory and on-disk.
- **Day scale, wide table**: 1000 rows/minute for one day — 1 440 000 rows,
  12 columns with 5 secondary indexes propagated via the template table.
- **Retention**: dropping one 1-hour partition (60 000 rows) vs the equivalent
  ranged `DELETE` on the plain table.

## Headline results

| Workload | Partitioned | Plain | Ratio |
|---|---|---|---|
| Insert 10.08M narrow rows, disk | 117.8 s | 4044.1 s | **34× faster** |
| Insert 1.44M wide rows, disk | 34.0 s | 793.7 s | **23× faster** |
| Insert 10.08M narrow rows, memory | 22.4 s | 29.1 s | 1.3× faster |
| Remove 60 000 rows (retention) | 3.3 ms (`DROP TABLE`) | 32.2 ms (`DELETE`) | **~10× faster** |
| Count via `partitioner_count_between` | ~0.4 ms | — | **~8× faster** than plain count |

The disk insert numbers are the headline: partitioned inserts batch in
autocommit mode, while the plain table pays per-row journal/fsync. Retention
is roughly constant in partition size (a `DROP TABLE`), while a ranged
`DELETE` is O(rows) — at 24-hour partitions the gap widens accordingly.

Ad-hoc scans that don't constrain the partition column run at parity to
~1.5× slower; pure row-iteration microbenchmarks can be ~5× slower (virtual
table dispatch overhead, structural). Partitioning pays off on ingest,
retention, and time-ranged access patterns.

## Running it

Benchmarks are release-optimized (`profile.test` inherits `release`):

```bash
# week-scale narrow suite, in-memory
PARTITIONER_BENCH_MINUTES=10080 PARTITIONER_BENCH_STORAGE=memory \
  cargo test --release -- --ignored --nocapture

# same on disk
PARTITIONER_BENCH_MINUTES=10080 PARTITIONER_BENCH_STORAGE=disk \
  cargo test --release -- --ignored --nocapture
```

Workload knobs: `PARTITIONER_BENCH_MINUTES`, `PARTITIONER_BENCH_ROWS_PER_MINUTE`,
`PARTITIONER_BENCH_STORAGE=memory|disk`. See
[benchmark-results/run_campaign.sh](https://github.com/nuuskamummu/Sqlite3_partitioner/blob/master/benchmark-results/run_campaign.sh)
for the full campaign, and the report for per-query tables.

The insert batch size is compile-time configurable:

```bash
PARTITIONER_INSERT_BATCH_SIZE=5000 cargo build --release
```
