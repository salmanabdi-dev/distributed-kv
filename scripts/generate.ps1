$ErrorActionPreference = "Stop"

$module = "github.com/salmanabdi-dev/distributed-kv"

if (-not (Get-Command protoc -ErrorAction SilentlyContinue)) {
    throw "protoc was not found on PATH. Install Protocol Buffers first, then reopen the terminal."
}
if (-not (Get-Command protoc-gen-go -ErrorAction SilentlyContinue)) {
    throw "protoc-gen-go was not found. Run: go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2"
}
if (-not (Get-Command protoc-gen-go-grpc -ErrorAction SilentlyContinue)) {
    throw "protoc-gen-go-grpc was not found. Run: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1"
}

protoc `
  --go_out=. --go_opt="module=$module" `
  --go-grpc_out=. --go-grpc_opt="module=$module" `
  proto/kv.proto proto/raft.proto

Write-Host "Generated proto/kvpb and proto/raftpb."
