Compile lib
Load lib into sqlite3
Create virtual table [table_name] using partitioner([interval, E.G 1 hour], [column arguments]). One column needs to be marked as "partition_column", that should also have the data type timestamp. At the moment this type will be stored as TEXT on the actual table, which might change in the future. The partition column is also the only column that will be type checked, you may enter any format as long as it is parsable as datetime
Supported datetime formats:

"%Y-%m-%d %H:%M:%S", // Standard ISO 8601 datetime

"%Y-%m-%d %H:%M", // ISO 8601 without seconds

"%Y-%m-%d", // ISO 8601 date only

"%d-%m-%Y %H:%M:%S", // European date with time

%d-%m-%Y %H:%M", // European date without seconds

"%d-%m-%Y", // European date

"%m/%d/%Y %H:%M:%S", // US date with time

"%m/%d/%Y %H:%M", // US date without seconds

"%m/%d/%Y", // US date

"%Y%m%d%H%M%S", // Compact datetime without separators

"%Y%m%d", // Compact date without separators

"%Y-%m-%dT%H:%M:%SZ", // ISO 8601 datetime with Zulu (UTC) time zone

"%Y-%m-%dT%H:%M:%S%z", // ISO 8601 datetime with numeric time zone

"%I:%M:%S %p", // 12-hour clock time with AM/PM

"%I:%M %p", // 12-hour clock time without seconds

"%B %d, %Y %H:%M:%S", // Full month name, day, year, time

"%b %d, %Y %H:%M:%S", // Abbreviated month name, day, year, time
UNIX epoch in seconds.

After creating a table using this extension, 3 shadow tables will be created in your database. Each table will be prefixed with the provided name and postfixed with: lookup, root, and template. These tables keeps track of which column is the partition column, how many underlying partitions exists etc. Sqlite3 does not support indexing of VTABs. To work around this, simply create your indices on the template shadow table. All these indexes, will be copied to the actual partitions when created. The partitions will be created automatically at given interval defined at creation. I.E, if you set the interval to 1 hour, the value of the partition column will be parsed as a UTC timestamp and clamped down to the nearest hour. A new partition will be created using this value (if it doesnt already exists) and the data will be inserted into this partition.
Right now all shadow tables are visible, but i plan to hide them away since altering them will have undefined behaviour.

Please note that this library is still experimental and should not be used in production. I'm not 100% confident in the datetime parser, insertions are done row by row, etc.

Known issues:

Full example of creating a table:
CREATE TABLE test USING partitioner(1 hour, col1 timestamp partition_column, col2 varchar);
