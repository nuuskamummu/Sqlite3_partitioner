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



##  Overview

The sqlite3-partitioner project is designed to enhance SQLite databases by introducing efficient data partitioning capabilities. Right now, only timeseries partitioning is supported. I hope to make other partitioning methods available in the future.
More information at https://nuuskamummu.github.io/Sqlite3_partitioner/




## Installation

Download the .so/.dylib from https://github.com/nuuskamummu/Sqlite3_partitioner/releases 

** partitioner-aarch64-apple-darwin.dylib
** partitioner-x86_64-unknown-linux-gnu.so
** no windows support yet, sorry!


> Start the SQLite3 command-line (needs to be compiled with the .load function). Or load into your application using a suitable driver
> ** macOS
> ```console
> $ .load PATH-DOWNLOADED-FILE/partitioner-aarch64-apple-darwin
> ```
> ** Linux
> ```console
> $ .load PATH-DOWNLOADED-FILE/partitioner-x86_64-unknown-linux-gnu
> ```

---

###  Usage

## Create
Use the CREATE VIRTUAL TABLE SQL command to define a new virtual table using the partitioner. Specify the partitioning interval (e.g., 1 hour) and the column arguments. Mark one column as the "partition_column," which will be used to determine the partitioning. This column should have the data type timestamp, but it will be stored as TEXT.
> ```console
> $ CREATE VIRTUAL TABLE test USING partitioner(
>    1 hour, 
>    col1 timestamp partition_column, 
>    col2 varchar
> );
> ```
Currently, the accepted interval formats are [integer] [hour] or [integer] [day]

## Insert

> ```console
> $ INSERT INTO test (col1, col2) VALUES ('2023-01-01 01:30:00', 'Sample Data');

## Indexing
Indexing are not supported by the Sqlite API, but a workaround exists. Visit https://nuuskamummu.github.io/Sqlite3_partitioner/usage/ for more information


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

