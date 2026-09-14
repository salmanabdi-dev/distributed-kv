package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/salmanabdi-dev/distributed-kv/internal/kv"
)

// SnapshotMeta describes a persisted snapshot's Raft boundary.
type SnapshotMeta struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}

// SnapshotStore persists full state-machine snapshots to a directory on
// disk (kept separate from the Pebble log/KV stores since snapshots are
// large, infrequent, whole-file writes rather than incremental KV writes -
// a plain file is simpler and avoids bloating the LSM tree with a huge
// value). Only the single most recent snapshot is retained; once a newer
// one is durably written, the previous one is removed.
type SnapshotStore struct {
	dir string
	mu  sync.Mutex
}

func NewSnapshotStore(dir string) (*SnapshotStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &SnapshotStore{dir: dir}, nil
}

func (s *SnapshotStore) metaPath() string    { return filepath.Join(s.dir, "snapshot.meta.json") }
func (s *SnapshotStore) dataPath() string    { return filepath.Join(s.dir, "snapshot.data") }
func (s *SnapshotStore) tmpDataPath() string { return filepath.Join(s.dir, "snapshot.data.tmp") }
func (s *SnapshotStore) tmpMetaPath() string { return filepath.Join(s.dir, "snapshot.meta.json.tmp") }

// Save writes a snapshot durably: data file first (fsynced), then the meta
// file (fsynced), both via write-tmp-then-rename so a crash mid-write never
// leaves a corrupt/partial snapshot visible to LoadLatest.
func (s *SnapshotStore) Save(snap kv.Snapshot, meta SnapshotMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := snap.Marshal()
	if err != nil {
		return err
	}

	if err := writeFileSync(s.tmpDataPath(), data); err != nil {
		return fmt.Errorf("writing snapshot data: %w", err)
	}
	if err := replaceFile(s.tmpDataPath(), s.dataPath()); err != nil {
		return fmt.Errorf("renaming snapshot data: %w", err)
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := writeFileSync(s.tmpMetaPath(), metaBytes); err != nil {
		return fmt.Errorf("writing snapshot meta: %w", err)
	}
	if err := replaceFile(s.tmpMetaPath(), s.metaPath()); err != nil {
		return fmt.Errorf("renaming snapshot meta: %w", err)
	}
	return nil
}

// LoadLatest returns the most recently saved snapshot, if one exists.
func (s *SnapshotStore) LoadLatest() (kv.Snapshot, SnapshotMeta, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	metaBytes, err := os.ReadFile(s.metaPath())
	if os.IsNotExist(err) {
		return kv.Snapshot{}, SnapshotMeta{}, false, nil
	}
	if err != nil {
		return kv.Snapshot{}, SnapshotMeta{}, false, err
	}
	var meta SnapshotMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return kv.Snapshot{}, SnapshotMeta{}, false, err
	}
	data, err := os.ReadFile(s.dataPath())
	if err != nil {
		return kv.Snapshot{}, SnapshotMeta{}, false, err
	}
	snap, err := kv.UnmarshalSnapshot(data)
	if err != nil {
		return kv.Snapshot{}, SnapshotMeta{}, false, err
	}
	return snap, meta, true, nil
}

// replaceFile renames src to dst, removing an existing destination first when
// necessary. os.Rename replaces files on Unix, but Windows returns Access is
// denied when dst already exists. SnapshotStore serializes Save calls, so there
// is no competing writer between the remove and rename steps.
func replaceFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(src, dst)
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// maybeSnapshot is invoked after applying an entry at an index that's a
// multiple of SnapshotThreshold. It is
// idempotent/safe to run concurrently with itself being a no-op if a
// snapshot at >= appliedIndex already exists.
func (n *Node) maybeSnapshot(appliedIndex uint64) {
	_, existingMeta, ok, err := n.snapStore.LoadLatest()
	if err == nil && ok && existingMeta.LastIncludedIndex >= appliedIndex {
		return
	}

	term, found := n.log.TermAt(appliedIndex)
	if !found {
		return
	}

	dump, err := n.sm.Dump()
	if err != nil {
		log.Printf("[%s] snapshot dump failed: %v", n.id, err)
		return
	}

	meta := SnapshotMeta{LastIncludedIndex: appliedIndex, LastIncludedTerm: term}
	if err := n.snapStore.Save(dump, meta); err != nil {
		log.Printf("[%s] snapshot save failed: %v", n.id, err)
		return
	}

	if err := n.log.CompactPrefix(appliedIndex, term); err != nil {
		log.Printf("[%s] log compaction failed: %v", n.id, err)
		return
	}

	log.Printf("[%s] snapshot complete at index %d (term %d)", n.id, appliedIndex, term)
}

// sendInstallSnapshot pushes the leader's most recent local snapshot to a
// follower that has fallen too far behind for normal log replication to
// catch it up (i.e. the entries it needs have already been compacted).
func (n *Node) sendInstallSnapshot(ctx context.Context, peerID string, term uint64) {
	snap, meta, ok, err := n.snapStore.LoadLatest()
	if err != nil || !ok {
		return
	}
	data, err := snap.Marshal()
	if err != nil {
		return
	}

	resp, err := n.transport.InstallSnapshot(ctx, peerID, &InstallSnapshotRequest{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: meta.LastIncludedIndex,
		LastIncludedTerm:  meta.LastIncludedTerm,
		Data:              data,
	})
	if err != nil {
		return
	}
	if resp.Term > term {
		n.becomeFollower(resp.Term, "", "")
		return
	}

	n.mu.Lock()
	if n.role == Leader && n.log.CurrentTerm() == term {
		n.nextIndex[peerID] = meta.LastIncludedIndex + 1
		n.matchIndex[peerID] = meta.LastIncludedIndex
	}
	n.mu.Unlock()
}

// HandleInstallSnapshot implements the InstallSnapshot RPC handler (Raft
// paper §7). Called by the server layer after translating from protobuf.
func (n *Node) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	currentTerm := n.log.CurrentTerm()
	if req.Term < currentTerm {
		return &InstallSnapshotResponse{Term: currentTerm}
	}
	if req.Term > currentTerm {
		_ = n.log.SetTermAndVote(req.Term, "")
		currentTerm = req.Term
	}
	n.mu.Lock()
	n.role = Follower
	n.leaderID = req.LeaderID
	n.mu.Unlock()
	n.resetElectionTimer()

	// Ignore stale/duplicate snapshots that don't advance our state.
	_, existingMeta, ok, _ := n.snapStore.LoadLatest()
	if ok && existingMeta.LastIncludedIndex >= req.LastIncludedIndex {
		return &InstallSnapshotResponse{Term: currentTerm}
	}

	snap, err := kv.UnmarshalSnapshot(req.Data)
	if err != nil {
		log.Printf("[%s] failed to unmarshal installed snapshot: %v", n.id, err)
		return &InstallSnapshotResponse{Term: currentTerm}
	}

	meta := SnapshotMeta{LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm}
	if err := n.snapStore.Save(snap, meta); err != nil {
		log.Printf("[%s] failed to save installed snapshot: %v", n.id, err)
		return &InstallSnapshotResponse{Term: currentTerm}
	}

	if err := n.sm.Restore(snap, req.LastIncludedIndex); err != nil {
		log.Printf("[%s] failed to restore state machine from snapshot: %v", n.id, err)
		return &InstallSnapshotResponse{Term: currentTerm}
	}

	if err := n.log.RestoreSnapshotBoundary(req.LastIncludedIndex, req.LastIncludedTerm); err != nil {
		log.Printf("[%s] failed to update log snapshot boundary: %v", n.id, err)
		return &InstallSnapshotResponse{Term: currentTerm}
	}

	n.mu.Lock()
	n.lastApplied = req.LastIncludedIndex
	n.commitIndex = req.LastIncludedIndex
	n.mu.Unlock()

	return &InstallSnapshotResponse{Term: currentTerm}
}
