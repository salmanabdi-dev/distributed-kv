// Package tests contains full-stack integration tests that exercise the
// real gRPC transport end-to-end (as opposed to internal/raft's own
// in-memory-transport tests). They therefore depend on the generated
// proto/kvpb and proto/raftpb packages - run ./scripts/generate.sh and
// `go mod tidy` first (see README "Setup"). They have NOT been executed in
// the sandbox that produced this repository, which has no Go toolchain or
// network access. Run with:
//
//	go test ./tests/...
//	go test -race ./tests/...
package tests

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salmanabdi-dev/distributed-kv/internal/kv"
	"github.com/salmanabdi-dev/distributed-kv/internal/raft"
	"github.com/salmanabdi-dev/distributed-kv/internal/server"
	"github.com/salmanabdi-dev/distributed-kv/internal/storage"
	"github.com/salmanabdi-dev/distributed-kv/proto/kvpb"
	"github.com/salmanabdi-dev/distributed-kv/proto/raftpb"
)

type testNode struct {
	id         string
	raftAddr   string
	clientAddr string
	dataDir    string

	raftEngine *storage.PebbleEngine
	kvEngine   *storage.PebbleEngine
	sm         *kv.StateMachine
	node       *raft.Node
	transport  *server.GRPCTransport

	raftGRPC   *grpc.Server
	clientGRPC *grpc.Server

	kvClient kvpb.KVServiceClient
	conn     *grpc.ClientConn

	stopOnce sync.Once
}

type testHarness struct {
	t     *testing.T
	nodes map[string]*testNode
	ids   []string
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// newHarness starts a real 3-node cluster: real Pebble storage under
// t.TempDir(), real Raft consensus, real gRPC servers bound to localhost
// on ephemeral ports, and real gRPC clients for talking to it - the same
// wiring as cmd/node/main.go.
func newHarness(t *testing.T, ids []string, snapshotThreshold uint64) *testHarness {
	t.Helper()
	h := &testHarness{t: t, nodes: make(map[string]*testNode), ids: ids}

	addrs := make(map[string]struct{ raft, client string })
	for _, id := range ids {
		addrs[id] = struct{ raft, client string }{
			raft:   fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			client: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		}
	}

	for _, id := range ids {
		h.startNode(id, ids, addrs, snapshotThreshold)
	}

	t.Cleanup(func() {
		for _, n := range h.nodes {
			h.stopNode(n.id)
		}
	})

	return h
}

func (h *testHarness) startNode(id string, allIDs []string, addrs map[string]struct{ raft, client string }, snapshotThreshold uint64) {
	t := h.t
	dataDir := filepath.Join(t.TempDir(), id)

	raftEngine, err := storage.OpenPebble(filepath.Join(dataDir, "raftlog"))
	if err != nil {
		t.Fatalf("open raft engine: %v", err)
	}
	kvEngine, err := storage.OpenPebble(filepath.Join(dataDir, "kvdata"))
	if err != nil {
		t.Fatalf("open kv engine: %v", err)
	}
	snapStore, err := raft.NewSnapshotStore(filepath.Join(dataDir, "snapshots"))
	if err != nil {
		t.Fatalf("open snapstore: %v", err)
	}
	sm := kv.NewStateMachine(kvEngine)

	peerRaftAddrs := make(map[string]string)
	var peers []raft.PeerInfo
	for _, other := range allIDs {
		if other == id {
			continue
		}
		peerRaftAddrs[other] = addrs[other].raft
		peers = append(peers, raft.PeerInfo{ID: other})
	}
	transport := server.NewGRPCTransport(peerRaftAddrs)

	node, err := raft.NewNode(id, peers, raft.Options{
		ElectionTimeoutMin:         100 * time.Millisecond,
		ElectionTimeoutMax:         200 * time.Millisecond,
		HeartbeatInterval:          30 * time.Millisecond,
		SnapshotThreshold:          snapshotThreshold,
		ReplicationBatchMaxEntries: 256,
		FsyncOnAppend:              true,
	}, transport, raftEngine, sm, snapStore, addrs[id].client)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	node.Start()

	raftLis, err := net.Listen("tcp", addrs[id].raft)
	if err != nil {
		t.Fatalf("listen raft: %v", err)
	}
	raftGRPC := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(raftGRPC, server.NewRaftServer(node))
	go raftGRPC.Serve(raftLis)

	clientLis, err := net.Listen("tcp", addrs[id].client)
	if err != nil {
		t.Fatalf("listen client: %v", err)
	}
	clientGRPC := grpc.NewServer()
	kvpb.RegisterKVServiceServer(clientGRPC, server.NewClientServer(node, sm))
	go clientGRPC.Serve(clientLis)

	conn, err := grpc.NewClient(addrs[id].client, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}

	h.nodes[id] = &testNode{
		id: id, raftAddr: addrs[id].raft, clientAddr: addrs[id].client, dataDir: dataDir,
		raftEngine: raftEngine, kvEngine: kvEngine, sm: sm, node: node, transport: transport,
		raftGRPC: raftGRPC, clientGRPC: clientGRPC,
		kvClient: kvpb.NewKVServiceClient(conn), conn: conn,
	}
}

// stopNode simulates a crash: it stops the Raft node and gRPC servers and
// closes storage, WITHOUT deleting on-disk data, so restartNode can bring
// it back with persisted state intact. Idempotent and safe to call more
// than once on the same *testNode (e.g. once explicitly by a
// failover/crash test, and again implicitly via the harness-wide
// t.Cleanup below) - only the first call actually stops/closes anything.
func (h *testHarness) stopNode(id string) {
	n, ok := h.nodes[id]
	if !ok {
		return
	}
	n.stopOnce.Do(func() {
		n.clientGRPC.Stop()
		n.raftGRPC.Stop()
		n.node.Stop()
		if n.transport != nil {
			if err := n.transport.Close(); err != nil {
				h.t.Errorf("closing transport for %s: %v", id, err)
			}
		}
		if err := n.raftEngine.Close(); err != nil {
			h.t.Errorf("closing raft engine for %s: %v", id, err)
		}
		if err := n.kvEngine.Close(); err != nil {
			h.t.Errorf("closing kv engine for %s: %v", id, err)
		}
		if n.conn != nil {
			_ = n.conn.Close()
		}
	})
}

// restartNode reopens the same on-disk data directory for id, simulating a
// process restart after a crash, and verifies persisted Raft/KV state is
// picked back up.
func (h *testHarness) restartNode(id string, snapshotThreshold uint64) {
	t := h.t
	old := h.nodes[id]

	addrs := make(map[string]struct{ raft, client string })
	for otherID, n := range h.nodes {
		addrs[otherID] = struct{ raft, client string }{raft: n.raftAddr, client: n.clientAddr}
	}
	addrs[id] = struct{ raft, client string }{raft: old.raftAddr, client: old.clientAddr}

	raftEngine, err := storage.OpenPebble(filepath.Join(old.dataDir, "raftlog"))
	if err != nil {
		t.Fatalf("reopen raft engine: %v", err)
	}
	kvEngine, err := storage.OpenPebble(filepath.Join(old.dataDir, "kvdata"))
	if err != nil {
		t.Fatalf("reopen kv engine: %v", err)
	}
	snapStore, err := raft.NewSnapshotStore(filepath.Join(old.dataDir, "snapshots"))
	if err != nil {
		t.Fatalf("reopen snapstore: %v", err)
	}
	sm := kv.NewStateMachine(kvEngine)

	peerRaftAddrs := make(map[string]string)
	var peers []raft.PeerInfo
	for otherID, a := range addrs {
		if otherID == id {
			continue
		}
		peerRaftAddrs[otherID] = a.raft
		peers = append(peers, raft.PeerInfo{ID: otherID})
	}
	transport := server.NewGRPCTransport(peerRaftAddrs)

	node, err := raft.NewNode(id, peers, raft.Options{
		ElectionTimeoutMin: 100 * time.Millisecond, ElectionTimeoutMax: 200 * time.Millisecond,
		HeartbeatInterval: 30 * time.Millisecond, SnapshotThreshold: snapshotThreshold,
		ReplicationBatchMaxEntries: 256, FsyncOnAppend: true,
	}, transport, raftEngine, sm, snapStore, old.clientAddr)
	if err != nil {
		t.Fatalf("new node on restart: %v", err)
	}
	node.Start()

	raftLis, err := net.Listen("tcp", old.raftAddr)
	if err != nil {
		t.Fatalf("relisten raft: %v", err)
	}
	raftGRPC := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(raftGRPC, server.NewRaftServer(node))
	go raftGRPC.Serve(raftLis)

	clientLis, err := net.Listen("tcp", old.clientAddr)
	if err != nil {
		t.Fatalf("relisten client: %v", err)
	}
	clientGRPC := grpc.NewServer()
	kvpb.RegisterKVServiceServer(clientGRPC, server.NewClientServer(node, sm))
	go clientGRPC.Serve(clientLis)

	conn, err := grpc.NewClient(old.clientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("redial: %v", err)
	}

	h.nodes[id] = &testNode{
		id: id, raftAddr: old.raftAddr, clientAddr: old.clientAddr, dataDir: old.dataDir,
		raftEngine: raftEngine, kvEngine: kvEngine, sm: sm, node: node, transport: transport,
		raftGRPC: raftGRPC, clientGRPC: clientGRPC,
		kvClient: kvpb.NewKVServiceClient(conn), conn: conn,
	}
}

// findLeader polls every node's client-facing Put/Get behavior indirectly
// by asking the raft.Node role directly (test-only shortcut; real clients
// use NOT_LEADER responses instead).
func (h *testHarness) findLeader(timeout time.Duration) *testNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range h.nodes {
			if n.node.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

func (h *testHarness) put(t *testing.T, leader *testNode, key, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := leader.kvClient.Put(ctx, &kvpb.PutRequest{Key: key, Value: []byte(value)})
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	if resp.Status != kvpb.StatusCode_OK {
		t.Fatalf("put %s: status=%v error=%s", key, resp.Status, resp.Error)
	}
}

func (h *testHarness) delete(t *testing.T, leader *testNode, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := leader.kvClient.Delete(ctx, &kvpb.DeleteRequest{Key: key})
	if err != nil {
		t.Fatalf("delete %s: %v", key, err)
	}
	if resp.Status != kvpb.StatusCode_OK {
		t.Fatalf("delete %s: status=%v error=%s", key, resp.Status, resp.Error)
	}
}

// getRaw performs a Get without asserting on the response, for tests that
// need to inspect the status code themselves (e.g. expecting NOT_LEADER).
func (h *testHarness) getRaw(t *testing.T, node *testNode, key string, linearizable bool) *kvpb.GetResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := node.kvClient.Get(ctx, &kvpb.GetRequest{Key: key, Linearizable: linearizable})
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return resp
}

func (h *testHarness) get(t *testing.T, node *testNode, key string, linearizable bool) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := node.kvClient.Get(ctx, &kvpb.GetRequest{Key: key, Linearizable: linearizable})
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	if resp.Status == kvpb.StatusCode_NOT_FOUND {
		return "", false
	}
	if resp.Status != kvpb.StatusCode_OK {
		t.Fatalf("get %s: status=%v error=%s", key, resp.Status, resp.Error)
	}
	return string(resp.Value), true
}
