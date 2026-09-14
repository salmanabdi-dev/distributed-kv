#!/usr/bin/env bash
# Generates Go code from the .proto definitions.
#
# NOT RUN in the sandbox that produced/maintains this repo (no network
# access to install protoc / the Go plugins there). Run this yourself once
# per machine, before the first build:
#
#   brew install protobuf                     # or apt-get install -y protobuf-compiler
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
#   export PATH="$PATH:$(go env GOPATH)/bin"
#   ./scripts/generate.sh
#
# This produces proto/kvpb/kv.pb.go, proto/kvpb/kv_grpc.pb.go,
# proto/raftpb/raft.pb.go, proto/raftpb/raft_grpc.pb.go - two separate
# packages, NOT merged into one directory, matching each .proto's
# go_package option (github.com/salmanabdi-dev/distributed-kv/proto/kvpb
# and .../proto/raftpb respectively).
#
# Once generated, COMMIT proto/kvpb and proto/raftpb to the repository
# (see README "Setup") so a fresh clone can `go build` without anyone
# needing protoc installed. Re-run this script and commit again whenever
# proto/kv.proto or proto/raft.proto change.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODULE="github.com/salmanabdi-dev/distributed-kv"

protoc \
  --go_out=. --go_opt=module="$MODULE" \
  --go-grpc_out=. --go-grpc_opt=module="$MODULE" \
  proto/kv.proto proto/raft.proto

echo "Generated proto/kvpb and proto/raftpb Go packages."
