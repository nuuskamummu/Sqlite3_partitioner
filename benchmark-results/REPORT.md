# Partitioner Benchmark Report

Generated: 2026-08-15, commit `b659114` (post ORDER BY pushdown / omit / partition-column index).

Hardware: local dev machine (macOS). Build: `--release` (opt-level 3, LTO). All numbers are
single-run wall times; treat as indicative, not lab-grade.

## Scenarios

- **Week scale (narrow table)**: 1000 rows/min × 10 080 min = **10 080 000 rows**,
  2-column table `(col1 timestamp, col2 text)`, hourly partitions (168 partitions).
  Run in-memory and on-disk.
- **Day scale (realistic wide table)**: 1000 rows/min × 1440 min = **1 440 000 rows**,
  12-column table, hourly partitions (24 partitions). Plain insert at week scale is
  impractical (~1.5h), hence the smaller window.
- **Retention**: drop one 1-hour partition (60 000 rows) vs equivalent ranged `DELETE`,
  week-scale DBs, file-backed (reopen between phases by design).

## Results

### Inserts

| Workload | Partitioned | Plain | Ratio |
|---|---|---|---|
| Narrow ×10.08M, memory | 22.4s | 29.1s | 1.3× faster |
| Narrow ×10.08M, disk | 117.8s | 4044.1s | **34× faster** |
| Wide ×1.44M, memory | 10.4s | 16.8s | 1.6× faster |
| Wide ×1.44M, disk | 34.0s | 793.7s | **23× faster** |

The disk numbers are the headline: partitioned inserts batch in autocommit mode, while the
plain table pays per-row journal/fsync. (Plain in an explicit transaction would close most
of this gap — but autocommit is the realistic default for callers.)

### Queries, week scale (10.08M rows, narrow)

| Query | Partitioned | Plain | Note |
|---|---|---|---|
| 2-hour range `count(*)` (memory) | 15.2ms | 3.1ms | vtab dispatch floor |
| 2-hour range `count(*)` (disk) | 85.0ms | 35.5ms | high run-to-run variance on disk |
| `partitioner_count_between` (memory) | 392µs | — | 8× faster than plain count |
| `partitioner_count_between` (disk) | 451µs | — | storage-independent (stats table) |
| Unindexed filter + range (memory) | 9.4ms | 8.8ms | parity |
| Unindexed filter + range (disk) | 111.2ms | 64.4ms | parity within noise; 12.3/10.9ms in an earlier contended run |
| Latest 1000 rows, `ORDER BY col1 DESC LIMIT` (memory) | 1.50ms | 103µs | ORDER BY consumed, no temp b-tree |
| Latest 1000 rows (disk) | 1.42ms | 127µs | |

Disk query numbers showed high variance between runs (10× on the unindexed filter),
likely page-cache and parallel-test contention; treat individual disk query timings as
order-of-magnitude only.

### Queries, day scale (1.44M rows, realistic wide)

| Query | Partitioned (mem/disk) | Plain (mem/disk) |
|---|---|---|
| 2-hour ts range count | 30.8 / 29.8ms | 3.1 / 4.6ms |
| 1-day status+region filter | 191.9 / 223.2ms | 119.4 / 208.3ms |
| 1-day device_id lookup | 2.26 / 3.50ms | 1.70 / 2.99ms |
| 1-week avg by category | 873 / 802ms | 574 / 539ms |

### Retention (week-scale DB, file-backed)

| Operation | Partitioned | Plain |
|---|---|---|
| Remove 60 000 rows (1 partition) | **3.3ms** (`DROP TABLE`) | 32.2ms (ranged `DELETE`) |

Drop cost is roughly constant in partition size; delete is O(rows). At 24h partitions
(1.44M rows) expect the gap to widen ~24×.

## Takeaways

- **Ingest**: partitioned wins everywhere, dramatically on disk (23-34×) thanks to
  autocommit insert batching.
- **Retention**: partition drop beats ranged delete ~10× at 60k rows, ~O(1) vs O(n).
- **Partition-aware counts** via `partitioner_count_between`: sub-millisecond regardless
  of table size.
- **Ad-hoc scans**: parity to ~1.5× slower when per-row work exists; ~5× slower on pure
  row-iteration microbenchmarks (vtab dispatch overhead, structural).
- **Memory vs disk**: disk amplifies the insert gap enormously; query ratios barely move
  (page cache warm in both modes at these sizes).

## Reproduce

```sh
# week-scale narrow suite, in-memory
PARTITIONER_BENCH_MINUTES=10080 PARTITIONER_BENCH_STORAGE=memory \
  cargo test --release -- --ignored --nocapture benchmarks::benchmark_partitioned_vs_plain_unindexed_filter benchmarks::benchmark_latest_rows_order_by benchmarks::benchmark_insert_partitioned_vs_plain benchmarks::benchmark_partitioned_vs_plain

# same on disk
PARTITIONER_BENCH_MINUTES=10080 PARTITIONER_BENCH_STORAGE=disk cargo test --release -- --ignored --nocapture <same filters>

# day-scale realistic wide table
PARTITIONER_BENCH_MINUTES=1440 PARTITIONER_BENCH_STORAGE={memory,disk} \
  cargo test --release benchmarks::benchmark_realistic_wide_table -- --ignored --nocapture --exact

# retention
PARTITIONER_BENCH_MINUTES=10080 \
  cargo test --release benchmarks::benchmark_partition_drop_vs_plain_delete -- --ignored --nocapture --exact
```

Raw logs: `benchmark-results/*.log` (see `run_campaign.sh`).

## Vector companion (sqlite-vec), 2026-08-18

Commit `af5b95b` (companion-driven scans). Workload: 1000 rows/min × 1440 min =
**1 440 000 rows** (hourly partitions, 24 pairs), dim-64 vectors, k=10. Three
contestants: the pure-SQL vtab one-liner (`WHERE emb MATCH ? AND k = 10 [AND
col1 window] ORDER BY distance`), hand-merged per-partition KNN + row
resolution, and a plain table with a manually synced global vec0 index.

| Query (memory) | vtab one-liner | Hand-merge | Plain global vec0 |
|---|---|---|---|
| Local KNN (1 partition) | 1.69ms | 1.65ms | 36.1ms |
| Global KNN (no time filter) | 37.7ms | 38.0ms | 36.1ms |
| Windowed KNN (2h) | **3.23ms** | 3.16ms | 109.2ms |

| Query (disk) | vtab one-liner | Hand-merge | Plain global vec0 |
|---|---|---|---|
| Local KNN (1 partition) | 5.06ms | 5.00ms | 114.0ms |
| Global KNN (no time filter) | 115.3ms | 116.6ms | 114.0ms |
| Windowed KNN (2h) | **9.59ms** | 9.66ms | 192.1ms |

Retention (one partition pair): partitioned drop 5.3ms memory / 15.3ms disk vs
853ms / 1.06s of ranged vector+row deletes on the plain schema (~60-160×).

Takeaways:

- The SQL-native vtab path matches the hand-written merge within noise (~1-3%)
  at every shape, in both storage modes — and it resolves full rows inline,
  which the hand-merge pays for on top.
- Windowed KNN (the target workload: similarity + time range) is **~34×
  (memory) / ~20× (disk)** faster than a global vec0 index with a rowid filter.
- Global KNN at this scale is parity even on disk (the earlier 2.88M-row sweep
  showed partitioned slower on disk there; check `vec_partitioned_sweep_*` logs
  before claiming parity at larger scales).

Reproduce:

```sh
VEC0_EXTENSION_PATH=/path/to/vec0 PARTITIONER_BENCH_MINUTES=1440 \
PARTITIONER_BENCH_STORAGE={memory,disk} \
  cargo test --features vec benchmark_partitioned_vec_companion_vs_plain \
  -- --ignored --nocapture
```

Raw logs: `benchmark-results/vtab_knn_1440000_{memory,disk}.log`.
