---
layout: default
title: Overview
permalink: /
nav_order: 0
---

# Overview

## Purpose

sqlite3-partitioner is a SQLite extension that adds declarative time-series
partitioning. You create one virtual table; the extension transparently splits
your rows into separate physical tables — one per time window — while your
application keeps querying a single ordinary-looking table.

## Who should use this?

Anyone accumulating time-stamped data in SQLite: metrics, logs, sensor
readings, events. If your table grows without bound and old data loses its
value, partitioning turns both ingest and retention into cheap operations.

## Why partition?

A monolithic time-series table degrades as it grows. Every insert maintains
ever-larger b-trees — one per index — and deleting old data is an
O(rows × indexes) operation that fragments the file. With partitioning:

- **Inserts stay flat.** Each partition has its own small b-trees; only the
  current partition is hot. Inserting into data from three months ago never
  touches the indexes of today's partition — and vice versa.
- **Retention is O(1).** Dropping an expired partition is a `DROP TABLE` —
  milliseconds regardless of row count — instead of a ranged `DELETE` that
  scales linearly and churns every index.
- **Range queries prune.** A `WHERE ts >= ... AND ts < ...` predicate skips
  partitions outside the window entirely, and `ORDER BY ts` is served from
  partition order with no external sort.

## How does it work internally?

You specify a partition column and an interval at `CREATE VIRTUAL TABLE`.
Alongside the virtual table, the extension creates shadow tables (ordinary
SQLite tables with reserved names):

- **`<name>_template`** — the schema blueprint. New partitions are created by
  copying it, indexes included. To add an index to your virtual table, create
  it on the template table; partitions created afterwards inherit it. The
  partition column itself is indexed automatically on every partition.
- **`<name>_lookup`** — maps each partition's start time (unix epoch seconds)
  to its physical table name, plus the partition's expiry timestamp when a
  lifetime is configured.
- **`<name>_root`** — the table's configuration: partition column, interval,
  lifetime.
- **`<name>_stats`** — per-partition row counts, used by
  `partitioner_count_between` and by the query planner's cost estimates.

Partition tables themselves are also shadow tables, named
`<name>_<partition start epoch>`. They share the template's columns and hold
the actual rows.

Retention is explicit: `SELECT partitioner_cleanup('<name>')` drops every
expired partition and removes its metadata from `_lookup` and `_stats`, all in
one transaction. You decide when the write lock is taken — nothing runs on a
schedule by itself.

## More info

See the [architecture](/architecture) page for the module layout, or read the
source code.
