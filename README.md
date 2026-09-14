# Distributed Key-Value Store

A linearizable, replicated key-value store built from scratch in Go. The system uses the Raft consensus algorithm for leader election and log replication, gRPC and Protocol Buffers for communication, and Pebble for persistent storage.

The project explores the core problems behind fault-tolerant distributed systems: maintaining consistency across nodes, surviving failures, persisting consensus state, recovering lagging replicas, and serving strongly consistent reads.

## Features

- Raft leader election and log replication
- Linearizable reads using ReadIndex
- Persistent Raft state and replicated key-value data
- gRPC communication between nodes and clients
- Snapshotting and Raft log compaction
- Snapshot-based recovery for lagging followers
- Leader failover and follower recovery
- Concurrent client writes
- Docker-based three-node deployment
- Unit, integration, failover, snapshot, and race-detection tests

## Architecture

The cluster consists of three independent nodes. Each node runs a Raft consensus instance, a persistent log, a key-value state machine, and two gRPC services.

```text
                         Client
                           |
                           v
                    +-------------+
                    |  KVService  |
                    +-------------+
                           |
                           v
                    +-------------+
                    |  Raft Node  |
                    +-------------+
                     /           \
                    /             \
             AppendEntries     AppendEntries
                  /                 \
                 v                   v
          +-------------+     +-------------+
          |   Node 2    |     |   Node 3    |
          |    Raft     |     |    Raft     |
          +-------------+     +-------------+

              Persistent storage on each node
                  Pebble + Snapshots
```

Each node exposes two services:

- **KVService** — client-facing `Put`, `Get`, and `Delete`
- **RaftService** — internal `RequestVote`, `AppendEntries`, and `InstallSnapshot`

The consensus implementation in `internal/raft` is separated from the gRPC layer. Raft communicates through a `Transport` interface, while `internal/server` adapts those requests to gRPC and Protocol Buffers.

This keeps the consensus algorithm independent of the network implementation and allows it to be tested with an in-process transport.

## Raft Consensus

The Raft implementation is located in `internal/raft`.

### Leader Election

Nodes begin as followers and use randomized election timeouts. If a follower stops receiving heartbeats, it becomes a candidate, increments its term, votes for itself, and requests votes from the other nodes.

A candidate becomes leader after receiving votes from a majority of the cluster.

The implementation includes:

- randomized election timeouts
- persistent terms and votes
- Raft log up-to-date checks
- term-based leader step-down
- majority-based elections

### Log Replication

Client writes are sent to the leader and appended to its Raft log.

The leader replicates entries to followers using `AppendEntries`. An entry becomes committed after it has been replicated to a majority of the cluster according to Raft's commit rules.

Replication includes:

- `prevLogIndex` / `prevLogTerm` consistency checks
- conflicting-entry detection
- conflict-term fast backtracking
- per-follower `nextIndex` and `matchIndex`
- majority-based commit advancement

Once committed, entries are applied to the key-value state machine.

## Client Request Flow

A typical write follows this path:

```text
Client
  |
  | Put(key, value)
  v
Leader
  |
  | append to Raft log
  |
  +-----------> Follower
  |
  +-----------> Follower
  |
  | majority replication
  v
Commit
  |
  v
Apply to state machine
  |
  v
Return OK
```

If a client contacts a follower for a write, the follower returns `NOT_LEADER` along with a leader hint rather than forwarding the request internally.

Once the client knows the current leader, future writes can be sent directly to it.

## Linearizable Reads

Strongly consistent reads use a ReadIndex-style approach instead of writing a no-op log entry for every read.

For a linearizable read, the leader:

1. Records its current commit index.
2. Confirms that it still holds leadership by contacting a majority of the cluster in the current term.
3. Waits until its state machine has applied through the required commit index.
4. Returns the requested value.

This provides strongly consistent reads without adding a new log entry for every `Get`.

Clients may also request non-linearizable reads from any node when stale data is acceptable.

## Persistent Storage

The project uses [Pebble](https://github.com/cockroachdb/pebble), an embedded LSM-tree key-value engine written in Go.

Each node maintains separate persistent stores for:

```text
raftlog/
    Raft log entries
    current term
    voted-for state
    snapshot metadata

kvdata/
    committed key-value data
```

The Raft layer depends on a small storage interface rather than directly depending on Pebble, allowing another storage engine to be substituted by implementing the same interface.

Raft metadata and log entries are persisted before being treated as durable.

## Snapshots and Log Compaction

Without compaction, the Raft log would grow indefinitely.

After a configurable number of applied entries, a node creates a snapshot containing:

- the current key-value state
- the last included Raft log index
- the last included Raft log term

Entries covered by the snapshot can then be compacted from the persistent log.

If a follower falls too far behind and the leader no longer has the required log entries, the leader sends an `InstallSnapshot` request instead of replaying unavailable history.

Nodes can also restore their state from local snapshots during startup.

## Fault Tolerance

The test suite exercises several failure and recovery scenarios:

| Scenario | Behavior |
|---|---|
| Follower failure | Remaining majority continues operating |
| Leader failure | Remaining nodes elect a new leader |
| Follower restart | Node catches up on missed writes |
| Old leader rejoins | Stale leader steps down after observing a newer term |
| Severely lagging follower | State recovered through snapshot installation |
| Node restart | Local state restored from persistent storage and snapshots |

These scenarios are implemented in the integration and Raft test suites.

## gRPC API

Protocol Buffer definitions live in:

```text
proto/kv.proto
proto/raft.proto
```

### KVService

Client-facing operations:

```text
Put
Get
Delete
```

### RaftService

Internal consensus operations:

```text
RequestVote
AppendEntries
InstallSnapshot
```

Generated Go bindings are stored in:

```text
proto/kvpb/
proto/raftpb/
```

## Project Structure

```text
distributed-kv/
├── cmd/
│   ├── benchmark/        # Benchmark client
│   └── node/             # Node executable
├── internal/
│   ├── config/           # Node configuration
│   ├── kv/               # Key-value state machine
│   ├── raft/             # Raft consensus implementation
│   ├── server/           # gRPC servers and transport
│   └── storage/          # Persistent storage abstraction
├── proto/                # Protocol Buffer definitions
├── scripts/              # Setup, generation, cluster, and benchmark scripts
├── tests/                # End-to-end integration tests
├── Dockerfile
├── docker-compose.yml
├── go.mod
└── go.sum
```

## Setup

### Requirements

- Go
- Protocol Buffers compiler (`protoc`)
- `protoc-gen-go`
- `protoc-gen-go-grpc`

Install the Go protobuf plugins:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Generate Protocol Buffers

Linux/macOS:

```bash
./scripts/generate.sh
```

Windows PowerShell:

```powershell
.\scripts\generate.ps1
```

Then resolve dependencies:

```bash
go mod tidy
```

## Testing

Run the complete test suite:

```bash
go test ./...
```

Run with Go's race detector:

```bash
go test -race ./...
```

On Windows, the project also includes a verification script:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\setup-and-test.ps1
```

The test suite covers persistent log behavior, elections, replication, concurrent writes, linearizable reads, leader failover, follower recovery, snapshots, and end-to-end communication over gRPC.

## Running the Cluster

The easiest way to start a three-node cluster is with Docker Compose:

```bash
docker compose up -d --build
```

View node logs:

```bash
docker compose logs -f
```

Stop a node to simulate failure:

```bash
docker compose stop node2
```

Restart it:

```bash
docker compose start node2
```

Stop the cluster and remove its persistent volumes:

```bash
docker compose down -v
```

## Benchmarking

A benchmark client is included in `cmd/benchmark`.

Example:

```bash
go run ./cmd/benchmark \
  -endpoints=127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 \
  -duration=60s \
  -warmup=5s \
  -concurrency=32 \
  -key-size=16 \
  -value-size=128 \
  -write-ratio=1.0
```

The benchmark can measure sustained and peak throughput under configurable concurrency, key size, value size, duration, and read/write workload.

A write is counted as successful only after the client receives an `OK` response following Raft commitment and application to the state machine.

## Design Tradeoffs

This project intentionally keeps several aspects simpler than a production distributed database.

**Static membership**

Cluster membership is configured at startup. Dynamic membership changes and Raft joint-consensus reconfiguration are not implemented.

**Snapshot generation**

Creating a state-machine snapshot currently blocks concurrent state-machine application while the keyspace is captured. A production implementation could use Pebble point-in-time snapshots to reduce this interruption.

**Client request deduplication**

Request IDs are carried through the write path, but the state machine does not maintain a replicated client deduplication table. A retry following a leadership change may therefore be applied more than once.

**Transport security**

The current gRPC transport uses insecure credentials and is intended for local or private-network experimentation. A production deployment would use authentication and TLS/mTLS.

**Single Raft group**

All keys are managed by one Raft group. Write throughput is therefore bounded by the leader and the replication/storage pipeline. Scaling beyond a single group's capacity would require partitioning the keyspace across multiple Raft groups.

## What I Learned

Building the system required working through several problems that are easy to hide behind existing distributed-systems libraries:

- coordinating concurrent state across multiple nodes
- reasoning about leader changes and stale terms
- maintaining persistent consensus state across restarts
- handling conflicting and missing log entries
- separating consensus logic from network transport
- recovering replicas after failures
- coordinating shutdown with persistent storage
- testing concurrent distributed behavior with Go's race detector

The project was built as a deeper exploration of backend infrastructure, distributed systems, concurrency, and fault-tolerant software.
