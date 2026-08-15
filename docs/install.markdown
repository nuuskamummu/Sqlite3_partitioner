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

Load it in the SQLite CLI (built with load-extension support):

```sql
-- macOS
.load PATH/partitioner-aarch64-apple-darwin

-- Linux
.load PATH/partitioner-x86_64-unknown-linux-gnu

-- Windows
.load PATH/partitioner-x86_64-pc-windows-msvc
```

Or load it from your application with `sqlite3_load_extension()` through your
driver of choice.

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
