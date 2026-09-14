#!/usr/bin/env bash
# Builds and starts the 3-node cluster via Docker Compose.
#
# Prerequisites (run once, see README "Setup"):
#   ./scripts/generate.sh   # produces proto/kvpb, proto/raftpb
#   go mod tidy             # populates go.sum
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [ ! -d proto/kvpb ] || [ ! -d proto/raftpb ]; then
  echo "error: generated protobuf code not found. Run ./scripts/generate.sh first." >&2
  exit 1
fi

docker compose up -d --build
echo "Cluster starting. Client endpoints: 127.0.0.1:9101, 127.0.0.1:9102, 127.0.0.1:9103"
echo "Tail logs with: docker compose logs -f"
