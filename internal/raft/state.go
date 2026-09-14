// Package raft implements the Raft consensus algorithm. It is deliberately
// independent of gRPC/protobuf: it defines its own request/response types
// and a Transport interface. internal/server bridges this to the generated
// RaftService gRPC client/server, so this package can be unit tested (and
// compiled) without generated protobuf code.
package raft

import (
	"context"
	"sync"
	"time"

	"github.com/salmanabdi-dev/distributed-kv/internal/kv"
	"github.com/salmanabdi-dev/distributed-kv/internal/storage"
)

// Role is one of the three Raft server states.
type Role int32

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// --- RPC request/response types (transport-agnostic) ---

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term             uint64
	LeaderID         string
	PrevLogIndex     uint64
	PrevLogTerm      uint64
	Entries          []Entry
	LeaderCommit     uint64
	LeaderClientAddr string
}

type AppendEntriesResponse struct {
	Term          uint64
	Success       bool
	ConflictTerm  uint64
	ConflictIndex uint64
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term uint64
}

// Transport lets a Node contact peers without knowing about gRPC. The
// server package implements this using generated RaftService clients.
type Transport interface {
	RequestVote(ctx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, peerID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

// PeerInfo is the minimal peer identity the raft package needs; addresses
// live in config/transport, not here.
type PeerInfo struct {
	ID string
}

// Options configures timing and batching behavior.
type Options struct {
	ElectionTimeoutMin         time.Duration
	ElectionTimeoutMax         time.Duration
	HeartbeatInterval          time.Duration
	SnapshotThreshold          uint64
	ReplicationBatchMaxEntries int
	FsyncOnAppend              bool
}

// Node is a single Raft participant.
type Node struct {
	mu sync.RWMutex

	// replicationMu serializes AppendEntries broadcast rounds. Multiple client
	// proposals may arrive concurrently, but overlapping replication rounds can
	// race on nextIndex/matchIndex and cause followers to receive stale ranges
	// out of order. Serializing the rounds preserves Raft log order while still
	// sending to different peers in parallel inside each round.
	replicationMu sync.Mutex

	id    string
	peers []PeerInfo // does NOT include self

	opts      Options
	transport Transport
	log       *PersistentLog
	sm        *kv.StateMachine
	snapStore *SnapshotStore

	role Role

	// Volatile state (all servers)
	commitIndex uint64
	lastApplied uint64

	// Volatile state (leaders only), reinitialized after each election
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// leaderID is the node this follower currently believes is leader
	// (best-effort, used only for client redirect hints).
	leaderID         string
	leaderClientAddr string

	// leaderClientAddrSelf is this node's own client-facing address,
	// forwarded to followers in AppendEntries so they can serve redirect
	// hints without a separate discovery protocol.
	selfClientAddr string

	electionResetCh chan struct{}

	// commitAdvancedCh is signaled (non-blocking) every time commitIndex
	// increases, so the apply loop can wake immediately instead of relying
	// solely on a polling interval. This matters directly for write
	// latency/throughput: without it, every write would incur up to one
	// full poll interval of extra latency between "committed" and
	// "applied" (the point Propose actually returns to the client).
	commitAdvancedCh chan struct{}

	// pendingReads holds ReadIndex requests waiting for lastApplied to
	// catch up to their recorded read index (see readindex.go).
	pendingReads []pendingRead

	// writeWaiters lets Propose block until the entry it appended at a
	// given index has actually been applied (or the log at that index was
	// overwritten by a new leader, in which case the waiter is failed).
	writeWaiters map[uint64]chan error

	stopCh chan struct{}
	wg     sync.WaitGroup
	// stopOnce guarantees close(stopCh) and the associated wg.Wait() only
	// ever run once, no matter how many times or from how many goroutines
	// Stop() is called concurrently. Without this, a second Stop() call
	// would panic with "close of closed channel" - this is a correctness
	// requirement for callers like test harnesses that may call Stop()
	// during both an explicit crash-simulation step and cleanup.
	stopOnce sync.Once

	rnd *randSource
}

type pendingRead struct {
	readIndex uint64
	done      chan struct{}
}

// NewNode constructs a Node. It does not start any background goroutines;
// call Start for that. Loading persisted state happens here so
// CurrentTerm/VotedFor/log contents are correct immediately.
func NewNode(id string, peers []PeerInfo, opts Options, transport Transport, engine storage.Engine, sm *kv.StateMachine, snapStore *SnapshotStore, selfClientAddr string) (*Node, error) {
	log, err := OpenPersistentLog(engine)
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:               id,
		peers:            peers,
		opts:             opts,
		transport:        transport,
		log:              log,
		sm:               sm,
		snapStore:        snapStore,
		role:             Follower,
		nextIndex:        make(map[string]uint64),
		matchIndex:       make(map[string]uint64),
		electionResetCh:  make(chan struct{}, 1),
		commitAdvancedCh: make(chan struct{}, 1),
		writeWaiters:     make(map[uint64]chan error),
		stopCh:           make(chan struct{}),
		rnd:              newRandSource(id),
		selfClientAddr:   selfClientAddr,
	}

	// Restore lastApplied/state machine from the most recent local
	// snapshot, if any, before replaying any remaining log (there should be
	// none beyond the snapshot boundary unless the process crashed between
	// snapshotting and compaction, which is handled defensively - the log
	// entries at/below lastIncludedIndex are simply skipped on replay).
	if snap, meta, ok, err := snapStore.LoadLatest(); err != nil {
		return nil, err
	} else if ok {
		if err := sm.Restore(snap, meta.LastIncludedIndex); err != nil {
			return nil, err
		}
		n.lastApplied = meta.LastIncludedIndex
		n.commitIndex = meta.LastIncludedIndex
	}

	return n, nil
}

func (n *Node) ID() string { return n.id }

func (n *Node) Role() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role
}

func (n *Node) CurrentTerm() uint64 {
	return n.log.CurrentTerm()
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role == Leader
}

// LeaderHint returns the client-facing address of the node this node
// currently believes is leader, for NOT_LEADER redirects. May be empty if
// unknown.
func (n *Node) LeaderHint() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderClientAddr
}

func (n *Node) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}
