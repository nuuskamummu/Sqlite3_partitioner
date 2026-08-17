---
layout: default
title: Features
permalink: /features
nav_order: 3
---

# Features

- Time-series partitioning by timestamp column.
- Partition intervals: `[integer] hour` and `[integer] day`.
- Optional retention: `lifetime [integer] day` records an expiry per partition.
- Explicit cleanup: `partitioner_cleanup('table')` drops expired partitions and
  purges their lookup/stats rows in one transaction, returning the count
  dropped. Caller-triggered only — you control when the write lock is taken.
- Automatic creation of physical partition tables from a template schema.
- Automatic copying of indexes defined on the template table to new partitions.
- Automatic index on the partition column of every partition.
- Batched inserts: rows are buffered in memory and flushed as multi-row
  `INSERT`s; flush happens on read, commit, or disconnect.
- Compile-time configurable batch size via `PARTITIONER_INSERT_BATCH_SIZE`
  (default 1000).
- Partition pruning: predicates on the partition column restrict the scan to
  the partitions in range, and the planner is told which constraints are
  already enforced.
- `ORDER BY` pushdown on the partition column — answered by scanning
  partitions in order, no temp b-tree.
- `partitioner_count_between(table, start, end)` — fast row counts over a
  time range from per-partition statistics, without scanning rows.
- Companion shadow tables (experimental, `--features vec`): keep an external
  virtual table — e.g. sqlite-vec `vec0` for vector search — in sync with the
  data partitions, with automatic purge on retention cleanup.
- Cross-platform: prebuilt extensions for macOS (arm64), Linux (x86_64), and
  Windows (x86_64).
