#!/usr/bin/env bash
# Runs the primary 12,000 writes/sec benchmark against a running cluster.
#
# Usage:
#   ./scripts/benchmark.sh                     # against dockerized cluster
#   ./scripts/benchmark.sh 127.0.0.1:9101,...  # against custom endpoints
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

ENDPOINTS="${1:-127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103}"
mkdir -p results

go run ./cmd/benchmark \
  -endpoints="$ENDPOINTS" \
  -duration=60s \
  -warmup=5s \
  -concurrency=32 \
  -key-size=16 \
  -value-size=128 \
  -write-ratio=1.0 \
  -leader-only=true \
  -json-out=results/results.json

echo
echo "Full JSON: results/results.json"
echo "Paste the 'Sustained writes/sec' figure above into RESULTS.md."
