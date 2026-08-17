#!/bin/bash
# Scaling sweep for the per-partition vec companion benchmark (dim-64).
# Collects per-size logs into benchmark-results/ for the tradeoff report.
# Usage: PARTITIONER_BENCH_STORAGE=disk ./vec_sweep.sh   (default: memory)
set -e
cd "$(dirname "$0")/.."
storage="${PARTITIONER_BENCH_STORAGE:-memory}"
for minutes in 720 1440 2880; do
  rows=$((minutes * 1000))
  log="benchmark-results/vec_partitioned_sweep_${rows}_${storage}.log"
  echo "=== ${rows} rows (${minutes} min, ${storage}) ==="
  VEC0_EXTENSION_PATH=/tmp/vec0.dylib \
  PARTITIONER_BENCH_STORAGE=$storage \
  PARTITIONER_BENCH_MINUTES=$minutes \
  PARTITIONER_BENCH_ROWS_PER_MINUTE=1000 \
  cargo test --features vec benchmark_partitioned_vec_companion -- --ignored --nocapture 2>&1 \
    | grep -E "ingest:|knn|retention|vector partitions|storage=" \
    | tee "$log"
done
echo "sweep done"
