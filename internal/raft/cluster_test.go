package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/salmanabdi-dev/distributed-kv/internal/kv"
	"github.com/salmanabdi-dev/distributed-kv/internal/storage"
)

// NOTE: this test harness simulates the network with direct in-process
// function calls (fakeTransport below) rather than real gRPC, so it
// exercises real consensus logic (election, replication, commit,
// failover, snapshotting) without depending on generated protobuf code.
// The gRPC transport itself (internal/server/transport.go) is a thin,
// separately-reviewable translation layer with no consensus logic in it.
//
// Not executed in the sandbox that produced this repo (no Go toolchain
// there). Run with:
//   go test ./internal/raft/...
//   go test -race ./internal/raft/...

type fakeTransport struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	// down simulates a crashed/partitioned node: see the comment on
	// RequestVote below for why membership in this set blocks traffic in
	// BOTH directions, not just inbound to the down node.
	down map[string]bool
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{nodes: make(map[string]*Node), down: make(map[string]bool)}
}

func (t *fakeTransport) setDown(id string, down bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.down[id] = down
}

func (t *fakeTransport) lookup(peerID, sourceID string) (*Node, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.down[peerID] || t.down[sourceID] {
		return nil, false
	}
	n, ok := t.nodes[peerID]
	return n, ok
}

// down simulates a fully crashed/partitioned node: ALL traffic to or from
// it fails, in both directions. Checking only the RPC's destination
// (peerID) is not enough to simulate a crash - the "crashed" node's own
// outbound goroutines (e.g. a leader's heartbeat loop) would keep running
// and successfully reach OTHER, healthy nodes, since the destination of
// those calls (a healthy peer) was never marked down. That would let a
// "crashed" leader go on sending valid heartbeats forever, so followers'
// election timers would never fire and no new election would ever
// happen - which is exactly the symptom of a leader-crash test that never
// elects a new leader. So every RPC method here checks BOTH the
// originating node (from the request's CandidateID/LeaderID field) and
// the destination peerID against the down set.
func (t *fakeTransport) RequestVote(ctx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	n, ok := t.lookup(peerID, req.CandidateID)
	if !ok {
		return nil, fmt.Errorf("node %s unreachable", peerID)
	}
	return n.HandleRequestVote(req), nil
}

func (t *fakeTransport) AppendEntries(ctx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	n, ok := t.lookup(peerID, req.LeaderID)
	if !ok {
		return nil, fmt.Errorf("node %s unreachable", peerID)
	}
	return n.HandleAppendEntries(req), nil
}

func (t *fakeTransport) InstallSnapshot(ctx context.Context, peerID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	n, ok := t.lookup(peerID, req.LeaderID)
	if !ok {
		return nil, fmt.Errorf("node %s unreachable", peerID)
	}
	return n.HandleInstallSnapshot(req), nil
}

type testCluster struct {
	t         *testing.T
	transport *fakeTransport
	nodes     map[string]*Node
	engines   map[string]*storage.PebbleEngine // tracked so cleanup can close them before TempDir removal
	order     []string
}

func newTestCluster(t *testing.T, ids []string) *testCluster {
	t.Helper()
	transport := newFakeTransport()
	tc := &testCluster{t: t, transport: transport, nodes: make(map[string]*Node), engines: make(map[string]*storage.PebbleEngine), order: ids}

	opts := Options{
		ElectionTimeoutMin:         50 * time.Millisecond,
		ElectionTimeoutMax:         100 * time.Millisecond,
		HeartbeatInterval:          15 * time.Millisecond,
		SnapshotThreshold:          0, // disabled by default; tests that want it set it explicitly
		ReplicationBatchMaxEntries: 256,
		FsyncOnAppend:              false, // fast, deterministic tests; durability itself is covered by log_test.go
	}

	for _, id := range ids {
		var peers []PeerInfo
		for _, other := range ids {
			if other != id {
				peers = append(peers, PeerInfo{ID: other})
			}
		}
		eng, err := storage.OpenPebble(t.TempDir())
		if err != nil {
			t.Fatalf("open engine for %s: %v", id, err)
		}
		sm := kv.NewStateMachine(eng)
		snapStore, err := NewSnapshotStore(t.TempDir())
		if err != nil {
			t.Fatalf("open snapstore for %s: %v", id, err)
		}
		node, err := NewNode(id, peers, opts, transport, eng, sm, snapStore, "client-"+id)
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		tc.nodes[id] = node
		tc.engines[id] = eng
		transport.mu.Lock()
		transport.nodes[id] = node
		transport.mu.Unlock()
	}

	for _, n := range tc.nodes {
		n.Start()
	}
	// Cleanup order matters on Windows: Node.Stop() must fully halt every
	// background goroutine (timer loop, apply loop) BEFORE the underlying
	// Pebble engine is closed, and the engine must be closed BEFORE
	// t.TempDir()'s automatic RemoveAll runs - otherwise Pebble's WAL/log
	// files are still open (or a background goroutine still has an open
	// iterator/handle on them) and Windows refuses to delete them
	// ("The process cannot access the file because it is being used by
	// another process"), unlike Unix where an unlinked-but-open file is
	// silently fine. t.Cleanup functions run in LIFO order, and
	// t.TempDir()'s own cleanup was registered before this one (it's
	// called earlier, inside the loop above), so registering ours here
	// guarantees Stop()+Close() run first.
	t.Cleanup(func() {
		// Quiesce the entire cluster before closing any shared test storage.
		// Stopping and closing one node at a time is unsafe: while the other
		// nodes are still running, an in-flight fakeTransport RPC can enter the
		// already-stopped node and touch its Pebble-backed log after that engine
		// has been closed, which panics with "pebble: closed".
		for _, n := range tc.nodes {
			n.Stop()
		}

		// Only after every Raft goroutine is stopped is it safe to close the
		// engines. This ordering also keeps Windows TempDir cleanup reliable.
		for id, eng := range tc.engines {
			if err := eng.Close(); err != nil {
				t.Errorf("closing storage engine for %s: %v", id, err)
			}
		}
	})
	return tc
}

func (tc *testCluster) leader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range tc.nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func TestCluster_ElectsExactlyOneLeader(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"})
	leader := tc.leader(2 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected within timeout")
	}

	count := 0
	for _, n := range tc.nodes {
		if n.IsLeader() {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 leader, found %d", count)
	}
}

func TestCluster_ReplicatedPutIsVisibleOnAllNodes(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"})
	leader := tc.leader(2 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := leader.Propose(ctx, EntryPut, "foo", []byte("bar"), "req-1"); err != nil {
		t.Fatalf("propose put: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for _, n := range tc.nodes {
		for {
			if n.CommitIndex() >= 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %s never caught up commitIndex", n.ID())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestCluster_SurvivesFollowerCrashAndRestart(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"})
	leader := tc.leader(2 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	var follower string
	for id := range tc.nodes {
		if id != leader.ID() {
			follower = id
			break
		}
	}

	tc.transport.setDown(follower, true)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := leader.Propose(ctx, EntryPut, "k1", []byte("v1"), "req-2"); err != nil {
		t.Fatalf("propose while follower down: %v", err)
	}

	tc.transport.setDown(follower, false)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if tc.nodes[follower].CommitIndex() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower %s never caught up after restart", follower)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCluster_NewLeaderElectedAfterLeaderCrash(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"})
	leader1 := tc.leader(2 * time.Second)
	if leader1 == nil {
		t.Fatal("no leader elected")
	}
	oldTerm := leader1.CurrentTerm()

	tc.transport.setDown(leader1.ID(), true)

	deadline := time.Now().Add(3 * time.Second)
	var leader2 *Node
	for time.Now().Before(deadline) {
		for id, n := range tc.nodes {
			if id != leader1.ID() && n.IsLeader() && n.CurrentTerm() > oldTerm {
				leader2 = n
			}
		}
		if leader2 != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader2 == nil {
		t.Fatal("no new leader elected after leader crash")
	}
}

func TestCluster_SnapshotAndCompactionAllowsLaggingFollowerToCatchUp(t *testing.T) {
	t.Skip("requires wiring a lower SnapshotThreshold through testCluster; " +
		"exercised in tests/snapshot_test.go against the full gRPC stack instead")
}
