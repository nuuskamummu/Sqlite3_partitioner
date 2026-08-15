#!/bin/bash
set -x
cd "$(dirname "$0")/.."
RUN="cargo test --release -- --ignored --nocapture"
NARROW="benchmarks::benchmark_partitioned_vs_plain_unindexed_filter benchmarks::benchmark_latest_rows_order_by benchmarks::benchmark_insert_partitioned_vs_plain benchmarks::benchmark_partitioned_vs_plain"

# A: memory, week (10.08M rows)
PARTITIONER_BENCH_MINUTES=10080 PARTITIONER_BENCH_STORAGE=memory \
  $RUN $NARROW > benchmark-results/week_memory_narrow.log 2>&1

# B: disk, week
PARTITIONER_BENCH_MINUTES=10080 PARTITIONER_BENCH_STORAGE=disk \
  $RUN $NARROW > benchmark-results/week_disk_narrow.log 2>&1

# C: drop vs delete, week (file-based by design)
PARTITIONER_BENCH_MINUTES=10080 \
  cargo test --release benchmarks::benchmark_partition_drop_vs_plain_delete -- --ignored --nocapture --exact \
  > benchmark-results/week_disk_drop.log 2>&1

# D: realistic wide table, one day (1.44M rows), memory
PARTITIONER_BENCH_MINUTES=1440 PARTITIONER_BENCH_STORAGE=memory \
  cargo test --release benchmarks::benchmark_realistic_wide_table -- --ignored --nocapture --exact \
  > benchmark-results/day_memory_realistic.log 2>&1

# E: realistic wide table, one day, disk
PARTITIONER_BENCH_MINUTES=1440 PARTITIONER_BENCH_STORAGE=disk \
  cargo test --release benchmarks::benchmark_realistic_wide_table -- --ignored --nocapture --exact \
  > benchmark-results/day_disk_realistic.log 2>&1
