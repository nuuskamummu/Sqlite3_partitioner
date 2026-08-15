---
layout: default
title: Architecture
permalink: /architecture
nav_order: 4
---

# Architecture

`sqlite3-partitioner` is a SQLite extension implemented in Rust. It exposes a
virtual table module that partitions incoming rows into separate physical
tables behind the scenes.

## Components

- **`src/vtab_interface`** — virtual table implementation (`create`, `connect`,
  `insert`, `update`, `delete`, `open`, `filter`, `best_index`, etc.). The
  planner hook (`best_index`) translates constraints on the partition column
  into partition ranges, reports them to SQLite via `omit`, and estimates
  costs from the `_stats` table.
- **`src/shadow_tables`** — backing tables that store metadata:
  - `_root` — partition column, interval, and lifetime.
  - `_template` — schema that new partitions copy, including indexes.
  - `_lookup` — partition start epoch → physical table name, plus expiry.
  - `_stats` — per-partition row counts used for query planning and
    `partitioner_count_between`.
- **`src/cleanup.rs`** — the `partitioner_cleanup` scalar function: drops
  expired partitions and purges their `_lookup`/`_stats` rows in one
  transaction.
- **`src/types`** — parsed column declarations, constraints, and WHERE-clause
  helpers.
- **`src/utils`** — interval parsing, value-type parsing, and datetime helpers.
- **`src/error`** — shared error types.

## How it works

1. `CREATE VIRTUAL TABLE` creates the shadow tables and records the
   partitioning configuration (interval, partition column, optional lifetime)
   in `_root`.
2. `INSERT` routes rows to an in-memory batch keyed by partition value. When
   the batch fills, or before a read/update/delete, the extension creates the
   target partition table (copying the template schema and indexes, plus the
   automatic partition-column index) and flushes the buffered rows as a
   multi-row insert.
3. `SELECT` uses `best_index` to turn `WHERE` constraints on the partition
   column into a partition range; only those physical tables are scanned.
   Because partitions are disjoint, ordered ranges of the partition column,
   `ORDER BY <partition column>` is answered by scanning partitions in order —
   no temp b-tree.
4. `UPDATE` and `DELETE` flush pending rows first, then use a rowid mapper to
   locate the physical row.
5. `partitioner_cleanup('<name>')` drops every partition whose expiry has
   passed (from `_lookup.expires_at`) and removes its metadata, returning the
   number of partitions dropped. It only runs when called.

## Repository structure

```sh
src/
├── error/              # error types
├── shadow_tables/      # root, template, lookup, stats, interface
├── types/              # column declarations, constraints
├── utils/              # parsing and validation helpers
├── vtab_interface/     # virtual table module and operations
├── benchmarks.rs       # ignored-by-default benchmark suite
├── cleanup.rs          # partitioner_cleanup scalar function
└── lib.rs
```
