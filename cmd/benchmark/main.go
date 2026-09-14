// Command benchmark drives real, replicated write/read load against a
// running distributed-kv cluster over gRPC, and reports sustained and peak
// throughput plus p50/p95/p99 latency. It intentionally does NOT use Go's
// testing.B micro-benchmark framework (per project requirements): this is
// a standalone load generator hitting the real network/replication path.
//
// A write is counted as successful only when the leader's PutResponse
// comes back with status OK, which - per the ClientServer implementation -
// only happens after Raft has committed AND applied the entry. Requests
// that receive NOT_LEADER are retried against the discovered leader and
// are not counted as either a success or a failure of that attempt; a
// request only counts as failed if it errors out or times out without
// ever completing.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salmanabdi-dev/distributed-kv/proto/kvpb"
)

type config struct {
	endpoints         string // comma-separated client addrs, e.g. node1:9101,node2:9102,node3:9103
	duration          time.Duration
	warmup            time.Duration
	concurrency       int
	keySize           int
	valueSize         int
	writeRatio        float64 // fraction of ops that are writes; rest are reads
	targetAllLeader   bool    // if true, always hit only the (discovered) leader; if false, distribute across all nodes to exercise redirect handling
	jsonOut           string
	linearizableReads bool
}

func main() {
	var cfg config
	flag.StringVar(&cfg.endpoints, "endpoints", "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103", "comma-separated client-facing addresses")
	flag.DurationVar(&cfg.duration, "duration", 60*time.Second, "total benchmark duration (including warmup)")
	flag.DurationVar(&cfg.warmup, "warmup", 5*time.Second, "warmup period excluded from sustained-throughput calculation")
	flag.IntVar(&cfg.concurrency, "concurrency", 32, "number of concurrent client goroutines")
	flag.IntVar(&cfg.keySize, "key-size", 16, "key size in bytes")
	flag.IntVar(&cfg.valueSize, "value-size", 128, "value size in bytes")
	flag.Float64Var(&cfg.writeRatio, "write-ratio", 1.0, "fraction of operations that are writes (1.0 = pure write benchmark)")
	flag.BoolVar(&cfg.targetAllLeader, "leader-only", true, "if true, all clients discover and send directly to the current leader; if false, clients hit random nodes and rely on NOT_LEADER redirects")
	flag.StringVar(&cfg.jsonOut, "json-out", "", "optional path to write results as JSON")
	flag.BoolVar(&cfg.linearizableReads, "linearizable-reads", true, "use ReadIndex-confirmed linearizable reads (slower, correct) vs local possibly-stale reads")
	flag.Parse()

	endpoints := strings.Split(cfg.endpoints, ",")
	for i := range endpoints {
		endpoints[i] = strings.TrimSpace(endpoints[i])
	}

	r := newRunner(cfg, endpoints)
	result := r.run()
	result.Print()

	if cfg.jsonOut != "" {
		b, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			log.Fatalf("marshal json: %v", err)
		}
		if err := os.WriteFile(cfg.jsonOut, b, 0o644); err != nil {
			log.Fatalf("write json: %v", err)
		}
		fmt.Printf("\nJSON results written to %s\n", cfg.jsonOut)
	}
}

// runner holds shared state across all worker goroutines.
type runner struct {
	cfg       config
	endpoints []string

	mu         sync.Mutex
	conns      map[string]*grpc.ClientConn
	leaderAddr string // best-known current leader address, shared across workers

	totalReqs   int64
	successReqs int64
	failedReqs  int64

	// perSecondCounts[i] = number of *successful writes* completed during
	// second i of the run (wall-clock, from start). Used to compute both
	// sustained (post-warmup average) and peak (best single second)
	// throughput without conflating the two.
	mu2             sync.Mutex
	perSecondWrites map[int64]int64

	latenciesMu          sync.Mutex
	writeLatenciesMicros []int64
	readLatenciesMicros  []int64

	startTime time.Time
}

func newRunner(cfg config, endpoints []string) *runner {
	return &runner{
		cfg:             cfg,
		endpoints:       endpoints,
		conns:           make(map[string]*grpc.ClientConn),
		perSecondWrites: make(map[int64]int64),
	}
}

func (r *runner) client(addr string) (kvpb.KVServiceClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.conns[addr]; ok {
		return kvpb.NewKVServiceClient(c), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	r.conns[addr] = conn
	return kvpb.NewKVServiceClient(conn), nil
}

func (r *runner) currentTarget(rnd *rand.Rand) string {
	if r.cfg.targetAllLeader {
		r.mu.Lock()
		addr := r.leaderAddr
		r.mu.Unlock()
		if addr != "" {
			return addr
		}
	}
	return r.endpoints[rnd.Intn(len(r.endpoints))]
}

func (r *runner) rememberLeader(addr string) {
	if addr == "" {
		return
	}
	r.mu.Lock()
	r.leaderAddr = addr
	r.mu.Unlock()
}

func (r *runner) run() *Result {
	r.startTime = time.Now()
	deadline := r.startTime.Add(r.cfg.duration)

	var wg sync.WaitGroup
	for i := 0; i < r.cfg.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r.worker(workerID, deadline)
		}(i)
	}
	wg.Wait()

	return r.buildResult()
}

func (r *runner) worker(workerID int, deadline time.Time) {
	rnd := rand.New(rand.NewSource(int64(workerID) ^ time.Now().UnixNano()))
	value := make([]byte, r.cfg.valueSize)
	rnd.Read(value)

	for time.Now().Before(deadline) {
		key := randomKey(rnd, r.cfg.keySize, workerID)
		isWrite := rnd.Float64() < r.cfg.writeRatio

		target := r.currentTarget(rnd)
		client, err := r.client(target)
		if err != nil {
			atomic.AddInt64(&r.failedReqs, 1)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var leaderHint string
		var opErr error
		start := time.Now()

		if isWrite {
			resp, err := client.Put(ctx, &kvpb.PutRequest{Key: key, Value: value})
			if err == nil {
				switch resp.Status {
				case kvpb.StatusCode_OK:
					r.recordSuccess(true, time.Since(start))
				case kvpb.StatusCode_NOT_LEADER:
					leaderHint = resp.LeaderHint
					opErr = fmt.Errorf("not leader")
				default:
					opErr = fmt.Errorf("status %v: %s", resp.Status, resp.Error)
				}
			} else {
				opErr = err
			}
		} else {
			resp, err := client.Get(ctx, &kvpb.GetRequest{Key: key, Linearizable: r.cfg.linearizableReads})
			if err == nil {
				switch resp.Status {
				case kvpb.StatusCode_OK, kvpb.StatusCode_NOT_FOUND:
					r.recordSuccess(false, time.Since(start))
				case kvpb.StatusCode_NOT_LEADER:
					leaderHint = resp.LeaderHint
					opErr = fmt.Errorf("not leader")
				default:
					opErr = fmt.Errorf("status %v: %s", resp.Status, resp.Error)
				}
			} else {
				opErr = err
			}
		}
		cancel()

		atomic.AddInt64(&r.totalReqs, 1)

		if opErr != nil {
			if leaderHint != "" {
				r.rememberLeader(leaderHint)
				// Redirect, not a hard failure: retry immediately without
				// counting it as a failed request.
				continue
			}
			atomic.AddInt64(&r.failedReqs, 1)
		}
	}
}

func (r *runner) recordSuccess(isWrite bool, latency time.Duration) {
	atomic.AddInt64(&r.successReqs, 1)
	micros := latency.Microseconds()

	r.latenciesMu.Lock()
	if isWrite {
		r.writeLatenciesMicros = append(r.writeLatenciesMicros, micros)
	} else {
		r.readLatenciesMicros = append(r.readLatenciesMicros, micros)
	}
	r.latenciesMu.Unlock()

	if isWrite {
		sec := time.Since(r.startTime).Milliseconds() / 1000
		r.mu2.Lock()
		r.perSecondWrites[sec]++
		r.mu2.Unlock()
	}
}

// randomKey returns a key of exactly `size` bytes: a worker-ID prefix (so
// different workers' keyspaces don't collide) padded/truncated with random
// hex to reach the requested size exactly.
func randomKey(rnd *rand.Rand, size int, workerID int) string {
	prefix := fmt.Sprintf("w%d-", workerID)
	if len(prefix) >= size {
		return prefix[:size]
	}
	remaining := size - len(prefix)
	b := make([]byte, (remaining+1)/2)
	rnd.Read(b)
	hex := fmt.Sprintf("%x", b)
	return (prefix + hex)[:size]
}

// Result is the JSON-serializable benchmark output.
type Result struct {
	Config struct {
		Endpoints         []string `json:"endpoints"`
		DurationSeconds   float64  `json:"duration_seconds"`
		WarmupSeconds     float64  `json:"warmup_seconds"`
		Concurrency       int      `json:"concurrency"`
		KeySizeBytes      int      `json:"key_size_bytes"`
		ValueSizeBytes    int      `json:"value_size_bytes"`
		WriteRatio        float64  `json:"write_ratio"`
		LinearizableReads bool     `json:"linearizable_reads"`
	} `json:"config"`

	TotalRequests      int64 `json:"total_requests"`
	SuccessfulRequests int64 `json:"successful_requests"`
	FailedRequests     int64 `json:"failed_requests"`

	SustainedWritesPerSec float64 `json:"sustained_writes_per_sec"`
	PeakWritesPerSec      float64 `json:"peak_writes_per_sec"`

	WriteLatencyP50Micros int64 `json:"write_latency_p50_micros"`
	WriteLatencyP95Micros int64 `json:"write_latency_p95_micros"`
	WriteLatencyP99Micros int64 `json:"write_latency_p99_micros"`

	ReadLatencyP50Micros int64 `json:"read_latency_p50_micros,omitempty"`
	ReadLatencyP95Micros int64 `json:"read_latency_p95_micros,omitempty"`
	ReadLatencyP99Micros int64 `json:"read_latency_p99_micros,omitempty"`

	ErrorRate float64 `json:"error_rate"`

	TargetAchieved bool `json:"target_12000_writes_per_sec_achieved"`
}

func (r *runner) buildResult() *Result {
	res := &Result{}
	res.Config.Endpoints = r.endpoints
	res.Config.DurationSeconds = r.cfg.duration.Seconds()
	res.Config.WarmupSeconds = r.cfg.warmup.Seconds()
	res.Config.Concurrency = r.cfg.concurrency
	res.Config.KeySizeBytes = r.cfg.keySize
	res.Config.ValueSizeBytes = r.cfg.valueSize
	res.Config.WriteRatio = r.cfg.writeRatio
	res.Config.LinearizableReads = r.cfg.linearizableReads

	res.TotalRequests = atomic.LoadInt64(&r.totalReqs)
	res.SuccessfulRequests = atomic.LoadInt64(&r.successReqs)
	res.FailedRequests = atomic.LoadInt64(&r.failedReqs)
	if res.TotalRequests > 0 {
		res.ErrorRate = float64(res.FailedRequests) / float64(res.TotalRequests)
	}

	warmupSec := int64(r.cfg.warmup.Seconds())
	totalSec := int64(r.cfg.duration.Seconds())

	r.mu2.Lock()
	var sustainedSum int64
	var sustainedSeconds int64
	var peak int64
	for sec, count := range r.perSecondWrites {
		if count > peak {
			peak = count
		}
		if sec >= warmupSec && sec < totalSec {
			sustainedSum += count
			sustainedSeconds++
		}
	}
	r.mu2.Unlock()

	if sustainedSeconds > 0 {
		res.SustainedWritesPerSec = float64(sustainedSum) / float64(sustainedSeconds)
	}
	res.PeakWritesPerSec = float64(peak)

	r.latenciesMu.Lock()
	res.WriteLatencyP50Micros = percentile(r.writeLatenciesMicros, 50)
	res.WriteLatencyP95Micros = percentile(r.writeLatenciesMicros, 95)
	res.WriteLatencyP99Micros = percentile(r.writeLatenciesMicros, 99)
	if len(r.readLatenciesMicros) > 0 {
		res.ReadLatencyP50Micros = percentile(r.readLatenciesMicros, 50)
		res.ReadLatencyP95Micros = percentile(r.readLatenciesMicros, 95)
		res.ReadLatencyP99Micros = percentile(r.readLatenciesMicros, 99)
	}
	r.latenciesMu.Unlock()

	res.TargetAchieved = res.SustainedWritesPerSec >= 12000

	return res
}

func percentile(data []int64, p int) int64 {
	if len(data) == 0 {
		return 0
	}
	sorted := append([]int64(nil), data...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (res *Result) Print() {
	fmt.Println("=== Benchmark Results ===")
	fmt.Printf("Total requests:      %d\n", res.TotalRequests)
	fmt.Printf("Successful:          %d\n", res.SuccessfulRequests)
	fmt.Printf("Failed:              %d\n", res.FailedRequests)
	fmt.Printf("Error rate:          %.4f%%\n", res.ErrorRate*100)
	fmt.Println()
	fmt.Printf("Sustained writes/sec: %.1f\n", res.SustainedWritesPerSec)
	fmt.Printf("Peak writes/sec:      %.1f\n", res.PeakWritesPerSec)
	fmt.Println()
	fmt.Printf("Write latency p50/p95/p99 (us): %d / %d / %d\n",
		res.WriteLatencyP50Micros, res.WriteLatencyP95Micros, res.WriteLatencyP99Micros)
	if res.ReadLatencyP50Micros > 0 {
		fmt.Printf("Read latency p50/p95/p99 (us):  %d / %d / %d\n",
			res.ReadLatencyP50Micros, res.ReadLatencyP95Micros, res.ReadLatencyP99Micros)
	}
	fmt.Println()
	fmt.Printf(">= 12,000 sustained writes/sec target achieved: %v\n", res.TargetAchieved)
}
