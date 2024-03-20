<p align="center">

</p>
<p align="center">
    <h1 align="center"></h1>
</p>
<p align="center">
	<!-- local repository, no metadata badges. -->
<p>
<p align="center">
		<em>Developed with the software and tools below.</em>
</p>
<p align="center">
	<img src="https://img.shields.io/badge/Rust-000000.svg?style=default&logo=Rust&logoColor=white" alt="Rust">
   <img src ="https://img.shields.io/badge/SQLite3-003B57.svg?style=flat&logo=SQLite&logoColor=white" alt="SQLite3"

</p>

<br><!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary><br>

- [ Overview](#-overview)
- [ Features](#-features)
- [ Repository Structure](#-repository-structure)
- [ Modules](#-modules)
- [ Getting Started](#-getting-started)
  - [ Installation](#-installation)
  - [ Usage](#-usage)
  - [ Tests](#-tests)
- [ Project Roadmap](#-project-roadmap)
- [ Contributing](#-contributing)
- [ License](#-license)
- [ Acknowledgments](#-acknowledgments)
</details>
<hr>

##  Overview

The sqlite3-partitioner project is designed to enhance SQLite databases by introducing efficient data partitioning capabilities. Right now, only timeseries partitioning is supported. I hope to make other partitioning methods available in the future.

---

##  Features

|    | Feature           | Description                                                                                                                                                                                                                                                                                      |
|----|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ⚙️  | **Architecture**  | Utilizes Rust and SQLite for a partitioning solution, focusing on efficient data segmentation. Incorporates a modular design with clear separations for error handling, types, utilities, shadow tables, and virtual table operations, ensuring a scalable and maintainable architecture.           |
| 🔩 | **Code Quality**  | Adheres to Rust's safety and performance principles, with a focus on type safety and efficient error handling. The organization of functionalities into modules and reusable components indicates high code quality and maintainability.                                                        |
| 📄 | **Documentation** | Each module and key functionality within the repository is described.                    |
| 🔌 | **Integrations**  | Integrates closely with SQLite for database operations by using the SQLite3 Virtual Table Module. Leverages Rust's ecosystem for type safety and performance.                        |
| 🧩 | **Modularity**    | High modularity through structuring the code into various modules. This allows for reusability and maintainability of the codebase, facilitating easy updates and feature expansion.                      |
| 🧪 | **Testing**       | Could be better to be honest.                                                       |
| ⚡️ | **Performance**   | Designed with performance in mind. Values are mostly passed by reference by leveraging Rust's safe and efficient borrowing rules.                               |
| 🛡️ | **Security**      | By using Rust, type safety and memory safety should be ensured. Dynamic SQL generation could benefit from further review to confirm adequate protection against SQL injection. |
| 📦 | **Dependencies**  | Key dependencies include Rust libraries and SQLite for database management and operations. Please refer to `Cargo.toml` and `Cargo.lock` for specific project dependencies.                                               |
| 🚀 | **Scalability**   | The architecture supports scalable database management operations through efficient data partitioning. The modular design facilitates easy expansion and adaptation to increased data volume or complexity without significant restructuring.                                   |
```

---

##  Repository Structure

```sh
└── /
    ├── Cargo.lock
    ├── Cargo.toml
    ├── LICENSE
    ├── notice.txt
    └── src
        ├── error
        ├── lib.rs
        ├── shadow_tables
        ├── types
        ├── utils
        └── vtab_interface
```

---

##  Modules

<details closed><summary>.</summary>

| File                     | Summary                                                                                                                                                                                                                                                                                                                                                                                             |
| ---                      | ---                                                                                                                                                                                                                                                                                                                                                                                                 |
| [Cargo.toml](Cargo.toml) | Defines the configuration for the sqlite3-partitioner project, specifying dependencies crucial for partitioning SQLite databases. It sets the foundation for the project's architecture, ensuring compatibility and optimal performance settings for the release. The `Cargo.toml` file integrates external libraries essential for extending SQLite functionalities within the repository's scope. |
| [notice.txt](notice.txt) | Grants legal use, distribution, and modification rights under the Apache License, Version 2.0, ensuring the repositorys compliance with open-source licensing.                                                            |

</details>

<details closed><summary>src</summary>

| File                 | Summary                                                                                                                                                                                                                                                                                                                                                 |
| ---                  | ---                                                                                                                                                                                                                                                                                                                                                     |
| [lib.rs](src/lib.rs) | Introduces and organizes core functionalities essential for the repositorys architecture, including error handling and interfaces for shadow tables, types, utilities, and virtual table operations. It facilitates access to pivotal components such as Lookup, LookupTable, RootTable, and TemplateTable, streamlining integration within the system. |

</details>

<details closed><summary>src.types</summary>

| File                                                     | Summary                                                                                                                                                                                                                                                                                                                                                                            |
| ---                                                      | ---                                                                                                                                                                                                                                                                                                                                                                                |
| [mod.rs](src/types/mod.rs)                               | Defines and implements serialization and deserialization logic for SQL value types and constraints, including custom behaviors for blob handling and SQL operation definitions, enabling seamless integration with SQLites type system and enhancing query formulation capabilities within the larger database management context.                                                 |
| [column_declaration.rs](src/types/column_declaration.rs) | Defines and manages table column specifications, including partition columns, in the database schema with functionalities for parsing, creating, and displaying column declarations. It facilitates schema definition by allowing the representation and manipulation of column attributes such as name, data type, and partitioning behavior within the repositorys architecture. |

</details>

<details closed><summary>src.types.constraints</summary>

| File                                                       | Summary                                                                                                                                                                                                                                                                                                                                      |
| ---                                                        | ---                                                                                                                                                                                                                                                                                                                                          |
| [mod.rs](src/types/constraints/mod.rs)                     | Enables dynamic query generation through the conversion of high-level constraint specifications into query conditions, utilizing `WhereClause` and `ValueRef` to construct complex, flexible database queries, thereby enhancing the repositorys ability to handle varied and dynamic data retrieval and manipulation scenarios efficiently. |
| [conditions.rs](src/types/constraints/conditions.rs)       | Defines and manages SQL WHERE clause conditions, allowing for the aggregation and inspection of multiple query constraints. It introduces structures to represent individual conditions and collections thereof, streamlining the process of constructing and applying complex query filters within the database applications architecture.  |
| [where_clauses.rs](src/types/constraints/where_clauses.rs) | Defines and manages SQL WHERE clause conditions, enabling dynamic query construction with support for serialized column conditions. Offers both single and aggregated conditions via `WhereClause` and `WhereClauses` structures, facilitating complex query parameterization and optimization within the database interaction layer.        |

</details>

<details closed><summary>src.utils</summary>

| File                                     | Summary                                                                                                                                                                                                                                                                                                                                                    |
| ---                                      | ---                                                                                                                                                                                                                                                                                                                                                        |
| [parsing.rs](src/utils/parsing.rs)       | Provides utility functions for parsing SQLite data types, including conversion between textual and UNIX epoch formats, facilitating seamless integration with SQLites dynamic typing. It enhances the librarys ability to handle datetime conversions, interval computations, and the generation of range queries based on given conditions.               |
| [mod.rs](src/utils/mod.rs)               | Central to the repositorys functionality, src/utils/mod.rs consolidates key operations for parsing and validation, streamlining data handling across the system. It enables efficient data interpretation and integrity checks, ensuring reliable and secure data manipulation within the projects architecture.                                           |
| [validation.rs](src/utils/validation.rs) | Validates columns against their declarations and identifies the partition column within input data, ensuring data type consistency primarily for partitioning logic. Critical in operations requiring schema adherence, it facilitates accurate input mapping and validation, pivotal for partitioned data processing within the repositorys architecture. |

</details>

<details closed><summary>src.shadow_tables</summary>

| File                                                     | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ---                                                      | ---                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| [interface.rs](src/shadow_tables/interface.rs)           | Introduces a `VirtualTable` class that enables efficient management of partitioned virtual tables in SQLite, including creation, connection, data manipulation, and destruction, with partitioning capabilities leveraging template, root, and lookup tables for dynamic data organization and access.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [template_table.rs](src/shadow_tables/template_table.rs) | Defines a `TemplateTable` class for creating and managing template tables in a database, enabling easy data partitioning and replication through methods to create, connect, copy, and manage table indices, supporting scalable and efficient data management within the systems architecture.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [mod.rs](src/shadow_tables/mod.rs)                       | Defines and manages shadow tables with a focus on partitioning logic, enabling the dynamic categorization of data based on predefined intervals, and seamlessly integrates these mechanisms within the repositorys architecture to support advanced database operations and optimizations.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [root_table.rs](src/shadow_tables/root_table.rs)         | Defines and manages the RootTable, crucial for the database partitioning scheme within the repositorys architecture, enabling dynamic data partitioning based on specified intervals and the partition column, thereby facilitating efficient and scalable data management.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| [lookup_table.rs](src/shadow_tables/lookup_table.rs)     | Generating SQL queries necessary for creating and populating lookup tables.-Connecting to existing lookup tables within the database.-Managing partition information, which is fundamental for database performance tuning and efficient data storage.By leveraging Rust's type system, traits, and the `sqlite3_ext` library, `lookup_table.rs` provides a robust framework for interacting with SQLite databases. This framework notably supports advanced database operations like partitioning through a clean and safe API. The involvement of types such as `Statement`, `Connection`, `Value`, and utility functions from the `sqlite3_ext` crate underline the file's focus on database interaction and manipulation.Within the context of the repositorys architecture, this file contributes to the database layer, enabling advanced features and optimizations in data handling and storage strategies. It's designed to work seamlessly with the surrounding modules, such as error handling, types, and utility functions, to provide a comprehensive solution for database management in Rust applications. This alignment with the repository's structure ensures that the codebase remains modular, maintainable, and scalable. |
| [operations.rs](src/shadow_tables/operations.rs)         | Defines and implements operations for managing database tables, including creation, connection, modification, and removal, with a focus on schema representations and table operations within an SQLite environment. It encapsulates table schema definitions, index adjustments, and SQL statement generation for seamless table manipulation.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

</details>

<details closed><summary>src.shadow_tables.partition_interface</summary>

| File                                                               | Summary                                                                                                                                                                                                                                                                                                            |
| ---                                                                | ---                                                                                                                                                                                                                                                                                                                |
| [partition.rs](src/shadow_tables/partition_interface/partition.rs) | Introduces a `Partition` structure for managing and iterating over segmented database queries within the repositorys architecture, leveraging dynamic SQL based on specified conditions to efficiently access and manipulate data in a partitioned manner, enhancing the data handling capabilities of the system. |
| [mod.rs](src/shadow_tables/partition_interface/mod.rs)             | Defines a partition interface within the shadow tables module, essential for segmenting data in a way that optimizes access and organization within the broader context of the repositorys architecture. Focuses on enhancing data management and retrieval efficiency in relation to the virtual table interface. |

</details>

<details closed><summary>src.error</summary>

| File                       | Summary                                                                                                                                                                                                                                                                                                                                    |
| ---                        | ---                                                                                                                                                                                                                                                                                                                                        |
| [mod.rs](src/error/mod.rs) | Defines a set of custom error types, particularly focusing on table operations within SQLite, such as column type mismatches and SQL errors. It facilitates error handling by integrating with SQLites error reporting mechanisms, ensuring smooth operation within the repositorys architecture aimed at enhancing database interactions. |

</details>

<details closed><summary>src.vtab_interface</summary>

| File                                                | Summary                                                                                                                                                                                                                                                                                                                                              |
| ---                                                 | ---                                                                                                                                                                                                                                                                                                                                                  |
| [vtab_module.rs](src/vtab_interface/vtab_module.rs) | Provides virtual table functionality for managing partitioned data within a SQLite database, enabling operations like insert, update, and delete. It leverages custom SQL schema creation, connection handling, and efficient data access strategies through partition-specific ID mapping and optimized query execution.                            |
| [vtab_cursor.rs](src/vtab_interface/vtab_cursor.rs) | Introduces a `RangePartitionCursor` for navigating and querying partitioned data efficiently in virtual tables, leveraging metadata for optimal data access, supporting seamless iteration across partitions and rows, and handling query conditions to dynamically adjust queried partitions based on specified bounds and conditions.              |
| [mod.rs](src/vtab_interface/mod.rs)                 | Initiates the Partitioner module within a SQLite database to efficiently manage partitioned tables using virtual table modules, ensuring thread safety. It also intelligently constructs SQL WHERE clauses from index information for optimal querying of virtual tables, leveraging a comprehensive understanding of table constraints and indexes. |

</details>

<details closed><summary>src.vtab_interface.operations</summary>

| File                                                 | Summary                                                                                                                                                                                                                                                                                                                                             |
| ---                                                  | ---                                                                                                                                                                                                                                                                                                                                                 |
| [create.rs](src/vtab_interface/operations/create.rs) | Connects to and creates virtual tables within a database, facilitating operations like querying and manipulation. It defines table structure, behavior, and ensures partitioning requirements are met, leveraging the repositorys architecture for error handling, shadow table interaction, and utility functions for parsing and type management. |
| [delete.rs](src/vtab_interface/operations/delete.rs) | Generates SQL DELETE statements for efficiently removing rows from database partitions, with support for both single and multiple row deletions through dynamic placeholder generation. This is integral to the repositorys data manipulation capabilities, particularly in managing and optimizing storage within its structured data environment. |
| [update.rs](src/vtab_interface/operations/update.rs) | Constructs an efficient SQL UPDATE statement for specific table partitions, determining and updating only changed values to optimize performance and maintain data integrity. Utilizes a smart comparison to skip unchanged columns, focusing updates on necessary data points within the virtual table architecture.                               |
| [mod.rs](src/vtab_interface/operations/mod.rs)       | Centralizes operations related to virtual table interfaces within the project, enabling creation, deletion, insertion, and updates. By aggregating these functions, it streamlines database interactions, ensuring a cohesive structure for managing data across the systems architecture.                                                          |
| [insert.rs](src/vtab_interface/operations/insert.rs) | Facilitates new row insertion into partitioned virtual tables by validating column data, determining the appropriate partition based on schema-defined intervals, and leveraging the `VirtualTable`s method to handle physical insertion, ensuring data placement adheres to partitioning rules.                                                    |

</details>

---

##  Getting Started

**System Requirements:**

* **Rust**: `version x.y.z`
* **Sqlite3**: `tested with 3.44, other versions have undefined behviour (will probably work tho)`

**Other Prerequisites**
* Basic knowledge of SQL and database concepts.

###  Installation

<h4>From <code>source</code></h4>

> 1. Clone the  repository:
>
> ```console
> $ git clone https://github.com/nuuskamummu/Sqlite3_partitioner
> ```
>
> 2. Change to the project directory:
> ```console
> $ cd Sqlite3_partitioner
> ```
>
> 3. Install the dependencies:
> ```console
> $ cargo build --release
> ```
> 4. Start the SQLite3 command-line tool and load the compiled library (needs to be compiled with the .load function). Or load into your application using a suitable driver
> ```console
> $ .load ./target/release/libpartitioner.dylib*
> ```

###  Usage
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


###  Tests

> Run the test suite using the command below:
> ```console
> $ cargo test
> ```

---

##  Project Roadmap

## Known limitations
The library is experimental and not recommended for production use without further development and testing.
The datetime parser may not handle all formats correctly; review and test thoroughly with your data.
Currently, all shadow tables are visible, and altering them can lead to undefined behavior. Plans to hide shadow tables are underway

---

##  Contributing

Contributions are welcome! Here are several ways you can contribute:

- **[Report Issues](https://local//issues)**: Submit bugs found or log feature requests for the `` project.
- **[Submit Pull Requests](https://local//blob/main/CONTRIBUTING.md)**: Review open PRs, and submit your own PRs.
- **[Join the Discussions](https://local//discussions)**: Share your insights, provide feedback, or ask questions.

<details closed>
<summary>Contributing Guidelines</summary>

1. **Fork the Repository**: Start by forking the project repository to your local account.
2. **Clone Locally**: Clone the forked repository to your local machine using a git client.
   ```sh
   git clone https://github.com/nuuskamummu/Sqlite3_partitioner
   ```
3. **Create a New Branch**: Always work on a new branch, giving it a descriptive name.
   ```sh
   git checkout -b new-feature-x
   ```
4. **Make Your Changes**: Develop and test your changes locally.
5. **Commit Your Changes**: Commit with a clear message describing your updates.
   ```sh
   git commit -m 'Implemented new feature x.'
   ```
6. **Push to local**: Push the changes to your forked repository.
   ```sh
   git push origin new-feature-x
   ```
7. **Submit a Pull Request**: Create a PR against the original project repository. Clearly describe the changes and their motivations.
8. **Review**: Once your PR is reviewed and approved, it will be merged into the main branch. Congratulations on your contribution!
</details>

<details closed>
<summary>Contributor Graph</summary>
<br>
<p align="center">
   <a href="https://local{//}graphs/contributors">
      <img src="https://contrib.rocks/image?repo=">
   </a>
</p>
</details>

---

##  License

This project is protected under the Apache 2.0 (https://www.apache.org/licenses/LICENSE-2.0) License. For more details, refer to the LICENSE file.

---

##  Acknowledgments
* https://github.com/CGamesPlay/sqlite3_ext

[**Back to the top**](#-overview)

---

