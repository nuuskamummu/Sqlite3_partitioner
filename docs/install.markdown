---
layout: page
title: Install
permalink: /install/
nav_order: 2
---

##  Getting Started

**System Requirements:**

* **Rust**: `version 1.7.x`
* **Sqlite3**: `tested with 3.44, other versions have undefined behaviour (will probably work tho)`

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
