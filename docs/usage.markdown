---
layout: page
title: Usage
permalink: /usage/
nav_order: 3
---

Use the CREATE VIRTUAL TABLE SQL command to define a new virtual table using the partitioner. Specify the partitioning interval (e.g., 1 hour) and the column arguments. Mark one column as the "partition_column," which will be used to determine the partitioning. This column should have the data type timestamp, but it will be stored as TEXT.
> ```console
> $ CREATE VIRTUAL TABLE test USING partitioner(
>    1 hour, 
>    col1 timestamp partition_column, 
>    col2 varchar
> );
> ```
Currently, the accepted interval formats are [integer] [hour] or [integer] [day]

**Supported Datetime Formats**
The partitioning library supports a wide range of datetime formats for the partition column, including:

* ISO 8601 datetime formats.
* European and US date formats.
* Compact datetime and date formats without separators.
* ISO 8601 with Zulu (UTC) time zone or numeric time zone.
* 12-hour clock time formats.
* Full and abbreviated month name formats.
* UNIX epoch in seconds.

**Shadow tables**
Upon creating a partitioned table, three shadow tables will be automatically generated in your database. These are prefixed with the provided table name (test in our example) and postfixed with lookup, root, and template. They manage partition metadata, such as the partition column and points to underlying partitions, which will also be created as shadow tables. Postfixed by a timestamp in UNIX epoch in seconds based on defined interval.

**Indexing**
SQLite3 does not support indexing of VTABs directly. To index your partitions, create the indexes on the template shadow table. These indexes will be copied to the actual partitions when they are created.

**Inserting Data**
Data insertion into the partitioned table is performed row by row. The value of the partition column is parsed as a UTC timestamp and adjusted to the nearest interval boundary defined during table creation. A new partition is automatically generated if it does not already exist, and the data is inserted into the appropriate partition. Planning on refining this behaviour.

> ```console
> $ INSERT INTO test (col1, col2) VALUES ('2023-01-01 01:30:00', 'Sample Data');
> ```

