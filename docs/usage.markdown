---
layout: page
title: Usage
permalink: /usage/
nav_order: 3
---

# Usage

## Creating a partitioned table

Use `CREATE VIRTUAL TABLE` to define a partitioned table. Specify the
partitioning interval and the column arguments, marking one `timestamp` column
as the `partition_column`:

```sql
CREATE VIRTUAL TABLE test USING partitioner(
   1 hour,
   col1 timestamp partition_column,
   col2 varchar
);
```

Accepted interval formats are `[integer] hour` and `[integer] day`.

## Retention

Add a `lifetime` clause to give each partition an expiry:

```sql
CREATE VIRTUAL TABLE test USING partitioner(
   1 day,
   lifetime 31 day,
   col1 timestamp partition_column,
   col2 varchar
);
```

Then drop expired partitions explicitly, whenever suits your application:

```sql
SELECT partitioner_cleanup('test');  -- returns number of partitions dropped
```

## Supported datetime formats

The extension accepts a wide range of datetime formats for the partition
column:

- ISO 8601 datetime formats.
- European and US date formats.
- Compact datetime and date formats without separators.
- ISO 8601 with Zulu (UTC) time zone or numeric time zone.
- 12-hour clock time formats.
- Full and abbreviated month name formats.
- UNIX epoch in seconds.

ISO `YYYY-MM-DD [HH:MM[:SS]]` is the safest, best-tested path.

## Shadow tables

Creating a partitioned table also creates shadow tables prefixed with the
table name (`test` in the example) and suffixed `_lookup`, `_root`,
`_template`, and `_stats`. They manage partition metadata; the partitions
themselves are shadow tables suffixed with the partition start as a UNIX epoch
in seconds. Do not alter shadow tables by hand — use `partitioner_cleanup`
for retention.

## Indexing

SQLite does not support indexing virtual tables directly. The partition column
is indexed automatically on every partition. For secondary indexes, create
them on the `_template` shadow table; they are copied to every partition
created afterwards (not retroactively to existing ones):

```sql
CREATE INDEX test_col2_idx ON test_template(col2);
```

## Inserting data

```sql
INSERT INTO test (col1, col2) VALUES ('2023-01-01 01:30:00', 'Sample Data');
```

The partition column value is parsed as a UTC timestamp and floored to the
interval boundary. The target partition is created on demand if it does not
exist. Inserts are buffered and flushed as multi-row batches; the batch size
is compile-time configurable via `PARTITIONER_INSERT_BATCH_SIZE`
(default 1000).

## Querying

Query the virtual table like any other table. Predicates on the partition
column prune partitions outside the range, and `ORDER BY` on the partition
column is served directly from partition order:

```sql
SELECT * FROM test WHERE col1 >= '2023-01-01' AND col1 < '2023-01-02'
ORDER BY col1 DESC LIMIT 100;
```

For fast counts over partition-aligned ranges without scanning rows:

```sql
SELECT partitioner_count_between('test', '2023-01-01', '2023-01-02');
```
