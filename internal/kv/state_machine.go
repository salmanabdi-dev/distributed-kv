// Package kv implements the replicated state machine that Raft-committed
// log entries are applied to. It is intentionally storage-engine-agnostic,
// depending only on internal/storage.Engine.
package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/salmanabdi-dev/distributed-kv/internal/storage"
)

// dataPrefix isolates committed KV data from any other keyspace sharing the
// same storage.Engine instance (e.g. if the state machine and Raft log ever
// share one Pebble DB in a deployment configuration).
var dataPrefix = []byte("d/")

func dataKey(key string) []byte {
	return append(append([]byte(nil), dataPrefix...), []byte(key)...)
}

// StateMachine is the deterministic, replicated key-value store. Every node
// applies the exact same sequence of committed operations to it, which is
// what gives the cluster consistent state.
type StateMachine struct {
	mu     sync.RWMutex
	engine storage.Engine

	// lastApplied is tracked here too (in addition to the Raft node's own
	// volatile lastApplied) purely as a consistency assertion aid; Raft
	// remains the source of truth for what has been applied.
	lastApplied uint64
}

func NewStateMachine(engine storage.Engine) *StateMachine {
	return &StateMachine{engine: engine}
}

// Get performs a direct read. Linearizability is enforced by the caller
// (server layer) via ReadIndex before invoking this, not by this method
// itself.
func (s *StateMachine) Get(key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, err := s.engine.Get(dataKey(key))
	if err == storage.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

// ApplyPut applies a committed PUT. index must be monotonically increasing
// across calls (enforced by the caller, the Raft apply loop).
func (s *StateMachine) ApplyPut(index uint64, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.engine.Set(dataKey(key), value, storage.WriteOptions{Sync: false}); err != nil {
		return err
	}
	s.lastApplied = index
	return nil
}

// ApplyDelete applies a committed DELETE.
func (s *StateMachine) ApplyDelete(index uint64, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.engine.Delete(dataKey(key), storage.WriteOptions{Sync: false}); err != nil {
		return err
	}
	s.lastApplied = index
	return nil
}

// ApplyNoop advances lastApplied for no-op entries (e.g. the leader's
// term-start no-op) without touching data.
func (s *StateMachine) ApplyNoop(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastApplied = index
}

func (s *StateMachine) LastApplied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastApplied
}

// Snapshot captures the entire current state machine content. It is used
// by the Raft layer's snapshot mechanism (internal/raft/snapshot.go) to
// serialize state for persistence and for InstallSnapshot RPCs.
type Snapshot struct {
	Pairs map[string][]byte
}

// Dump produces a full, consistent point-in-time snapshot of all KV pairs.
// Held under the write lock so it is atomic with respect to concurrent
// Apply calls (a real production system might instead use an MVCC/pebble
// snapshot to avoid blocking applies during the dump; documented as a
// known limitation in README's Tradeoffs section).
func (s *StateMachine) Dump() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	it, err := s.engine.NewIterator(dataPrefix, prefixUpperBound(dataPrefix))
	if err != nil {
		return Snapshot{}, err
	}
	defer it.Close()

	pairs := make(map[string][]byte)
	for it.Valid() {
		k := it.Key()[len(dataPrefix):]
		v := it.Value()
		pairs[string(k)] = append([]byte(nil), v...)
		it.Next()
	}
	return Snapshot{Pairs: pairs}, nil
}

// Restore replaces all current KV data with the contents of a snapshot.
// Used on startup (loading a local snapshot) and when installing a
// snapshot pushed by the leader.
func (s *StateMachine) Restore(snap Snapshot, appliedIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear existing data first.
	it, err := s.engine.NewIterator(dataPrefix, prefixUpperBound(dataPrefix))
	if err != nil {
		return err
	}
	b := s.engine.NewBatch()
	for it.Valid() {
		b.Delete(append([]byte(nil), it.Key()...))
		it.Next()
	}
	it.Close()

	for k, v := range snap.Pairs {
		b.Set(dataKey(k), v)
	}
	if err := b.Commit(storage.WriteOptions{Sync: true}); err != nil {
		return err
	}
	s.lastApplied = appliedIndex
	return nil
}

// Marshal/Unmarshal are used to serialize a Snapshot to bytes for the
// InstallSnapshot RPC payload and for on-disk snapshot files.
func (s Snapshot) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s.Pairs); err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

func UnmarshalSnapshot(data []byte) (Snapshot, error) {
	var pairs map[string][]byte
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&pairs); err != nil {
		return Snapshot{}, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return Snapshot{Pairs: pairs}, nil
}

func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			return upper[:i+1]
		}
	}
	return nil
}
