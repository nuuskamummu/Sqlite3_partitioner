#!/bin/bash
# Scaling sweep for the vec companion benchmark (in-memory, dim-64).
# Collects per-size logs into benchmark-results/ for the tradeoff report.
set -e
cd "$(dirname "$0")/.."
for minutes in 720 1440 2880; do
  rows=$((minutes * 1000))
  log="benchmark-results/vec_sweep_${rows}_memory.log"
  echo "=== ${rows} rows (${minutes} min) ==="
  VEC0_EXTENSION_PATH=/tmp/vec0.dylib \
  PARTITIONER_BENCH_MINUTES=$minutes \
  PARTITIONER_BENCH_ROWS_PER_MINUTE=1000 \
  cargo test --features vec benchmark_vec_companion -- --ignored --nocapture 2>&1 \
    | grep -E "similar-in-window|ingest:|knn|purge|vec row counts|test result" \
    | tee "$log"
done
echo "sweep done"
