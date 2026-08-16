---
layout: page
title: Install
permalink: /install/
nav_order: 2
---

# Install

## Prebuilt extension

Download the extension for your platform from
[Releases](https://github.com/nuuskamummu/Sqlite3_partitioner/releases):

- `partitioner-aarch64-apple-darwin.dylib` (macOS, Apple Silicon)
- `partitioner-x86_64-unknown-linux-gnu.so` (Linux)
- `partitioner-x86_64-pc-windows-msvc.dll` (Windows)

Load it in the SQLite CLI (built with load-extension support), passing the
init function explicitly:

```sql
-- macOS
.load PATH/partitioner-aarch64-apple-darwin sqlite3_partitioner_init

-- Linux
.load PATH/partitioner-x86_64-unknown-linux-gnu sqlite3_partitioner_init

-- Windows
.load PATH/partitioner-x86_64-pc-windows-msvc sqlite3_partitioner_init
```

Or load it from your application with `sqlite3_load_extension()` (entry point
`sqlite3_partitioner_init`) through your driver of choice.

## From source

System requirements:

- A stable Rust toolchain (edition 2021)
- SQLite 3 (developed and tested against 3.44; other versions are expected to
  work)

Clone and build:

```bash
git clone https://github.com/nuuskamummu/Sqlite3_partitioner
cd Sqlite3_partitioner
cargo build --release
```

Then load the compiled library from `target/release/`
(`libpartitioner.dylib` on macOS, `libpartitioner.so` on Linux,
`partitioner.dll` on Windows):

```sql
.load ./target/release/libpartitioner
```
