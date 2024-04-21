---
layout: default
title: Overview
permalink: /
nav_order: 0
---



##  Purpose
The sqlite3-partitioner project extends SQLite databases by incorporating efficient data partitioning capabilities specifically for time-series data. This extension is designed to improve database performance through effective data management, focusing initially on time-series partitioning with plans to support additional methods in the future.

## Who should use this?
Anyone who wants to improve performance by horizontally partitioning their SQLite database with timeseries partitions should give this extension a try.

## Why should anyone use this?
In most cases with timeseries databases, data quickly loses its value after insert. Insertions are usually very frequent aswell, over time this accumulates to large quantites of more or less useless data. While storage space might be cheap, concurrent reads, inserts, and deletions on the same table are not. Every time a record is deleted or inserted any index needs to be rebuilt. With a large enough table i know personal experience that this can lead to abyssmal performance. Not good. By partition data based on a specified interval, the number records per table will capped by the size of the interval. This means the table that contains data from three months (or even 1 hour) ago won't need to have it's indices restructured when you are inserting data right now. Furthermore, no need to delete rows (at least just because their best before date have expired), just drop the table instead! Much cheaper.

## Is it easy to use?
This extension is designed with ease of use in mind. Once your partition table is created, just query it as it would have been a normal SQLite table. However, the SQLite VTAB API have some limitations i had to work around.

## How does it work internally?
You specify a partition column and an interval at CREATE VIRTUAL TABLE. Along with your virtual table, multiple shadow tables (fancy word for normal tables) will also be created. 

* The template table, which contains the columns specified at creation. This is the table you should add indexes to. They will be copied over to any partitions along with the column declarations as soon as they are created. I.E to add an index to your virtual table, create it on the table postfixed with "_template" (quotation marks omitted). E.G if your virtual table is called test, the template table will be called test_template.

* The lookup table, this table keeps track of all existing partitions that have been created. It stores the name and the starting timestamp of the interval. I.E if the interval was set to "1 hour" when creating the virtual table, the timestamp values might be '2024-01-01 12:00', '2024-01-01-13:00' and so on. Only they are stored as unix epoch in seconds. It will also store the name of the partition, which will be the virtual table name postfixed with the starting timestamp of the interval in seconds since unix epoch. I will write this in caps because it is a somewhat IMPORTANT GOTCHA. RIGHT NOW, WHEN DROPPING A TABLE YOU HAVE TO DELETE THE CORRESPONDING ROW IN THE LOOKUP TABLE MANUALLY, OTHERWISE AN ERROR MIGHT OCCUR WHEN QUERYING. Internally this extension caches references to existing timeseries partitions in-memory during the lifetime of each database connection. if a partition is deleted but it's reference are still in the lookup table, the query engine might try to query a non-exisiting table! Not good! I will fix this. Submit a pull request on the github page if you have a fix, or open a discussion if you have an idea.

* Root table, holds information about names of the other shadow tables, and interval. Basically the timeseries partition metadata.

* Partition tables are also considered shadow tables by SQLite. Every table prefixed with the name specified at creation is a shadow table. These tables will have the same columns as the template table, but will actually hold data.

## More info
 If you want to know more about the intestines of this SQLite timeseries partitioning extension, see the architecture page. Or read the source code.


