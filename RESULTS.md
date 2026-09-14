# Benchmark & Correctness Results

**Status: NOT YET MEASURED.**

Every numeric field below is `Not yet measured`. This file was generated
in a sandboxed environment with no network access and no Go toolchain, so
no code here has been built, run, or benchmarked. Do not treat any figure
in this file as real until you have personally run the commands listed
under each section and replaced the placeholder with actual output.

Per the project's metric-integrity requirements: no benchmark output in
this file may be hard-coded, and only *sustained* (not peak) throughput
may be used for the final target claim.

---

## 1. System

| Field | Value |
|---|---|
| Node count | 3 |
| Consensus algorithm | Raft (custom implementation, `internal/raft`) |
| Storage engine | Pebble (embedded, pure-Go LSM) |
| gRPC configuration | Not yet measured (fill in: keepalive settings, max message size if changed from defaults, TLS on/off) |
| Go version | Not yet measured — run `go version` |

## 2. Hardware

| Field | Value |
|---|---|
| CPU | Not yet measured — run `lscpu` (Linux) or `sysctl -n machdep.cpu.brand_string` (macOS) |
| RAM | Not yet measured — run `free -h` (Linux) or `sysctl hw.memsize` (macOS) |
| OS | Not yet measured — run `uname -a` |

## 3. Workload

Fill in from the exact `cmd/benchmark` flags used for the reported run:

| Field | Value |
|---|---|
| Duration | Not yet measured |
| Warmup (excluded from sustained calc) | Not yet measured |
| Concurrency | Not yet measured |
| Key size | Not yet measured |
| Value size | Not yet measured |
| Total requests | Not yet measured |
| Read/write ratio | Not yet measured |
| `FSYNC_ON_APPEND` | Not yet measured (must be stated explicitly - see Durability note below) |
| Replication batch size | Not yet measured |

**Command used** (fill in with the actual invocation once run):

```bash
go run ./cmd/benchmark \
  -endpoints=127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103 \
  -duration=60s -warmup=5s -concurrency=<FILL IN> \
  -key-size=16 -value-size=128 -write-ratio=1.0 \
  -json-out=results/results.json
```

## 4. Performance

| Metric | Value |
|---|---|
| **Sustained writes/sec** | **Not yet measured** |
| Peak writes/sec | Not yet measured |
| p50 write latency | Not yet measured |
| p95 write latency | Not yet measured |
| p99 write latency | Not yet measured |
| Sustained reads/sec (if measured) | Not yet measured |
| Error rate | Not yet measured |

> Durability note: if the reported run used `FSYNC_ON_APPEND=false` for a
> higher-throughput comparison point, that MUST be stated directly above
> this table, since it trades a window of crash durability for
> throughput and the two modes are not comparable numbers.

## 5. Correctness

Run `go test ./... && go test -race ./...` and `go test ./tests/...` for
the specific fault-tolerance scenarios, then record actual outcomes:

| Scenario | Test | Result |
|---|---|---|
| Leader failure | `tests/failover_test.go::TestLeaderCrashNewLeaderElectedAndDataSurvives` | Not yet measured |
| Follower failure | `tests/failover_test.go::TestFollowerCrashClusterStillAvailable` | Not yet measured |
| Restart recovery | `tests/failover_test.go::TestFollowerRestartRecoversPersistedStateAndCatchesUp` | Not yet measured |
| Snapshot recovery | `tests/snapshot_test.go::TestSnapshotInstallationRecoversFarBehindFollower` | Not yet measured |
| Race detector (`go test -race ./...`) | all packages | Not yet measured |

## 6. Target

**Did this implementation achieve >= 12,000 sustained replicated writes/sec?**

`Not yet measured` — this claim can only be made after running
`scripts/benchmark.sh` (or the equivalent `cmd/benchmark` invocation
above) against a real 3-node cluster (local or Docker) and recording the
`sustained_writes_per_sec` field from its JSON output here, unedited.

If the first measured run does not reach the target, follow the
target-driven optimization loop in the project's Phase 13 plan (profile ->
identify bottleneck -> optimize -> re-measure) and log each iteration
below rather than overwriting this section, so the optimization history
stays auditable:

| Iteration | Change made | Sustained writes/sec measured |
|---|---|---|
| 0 (baseline) | Initial implementation, as committed | Not yet measured |
