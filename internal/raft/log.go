package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/salmanabdi-dev/distributed-kv/internal/storage"
)

// EntryType mirrors raftpb.EntryType without depending on generated code,
// so this file compiles standalone; the server layer converts to/from the
// protobuf type at the RPC boundary.
type EntryType int32

const (
	EntryNoop EntryType = iota
	EntryPut
	EntryDelete
)

// Entry is a single persisted Raft log entry.
type Entry struct {
	Index           uint64
	Term            uint64
	Type            EntryType
	Key             string
	Value           []byte
	ClientRequestID string
}

// Log-key layout (all under a single Pebble instance, isolated by prefix):
//
//	l/<index big-endian uint64>  -> gob-encoded Entry
//	m/currentTerm                -> uint64 big-endian
//	m/votedFor                   -> string bytes (empty = none)
//	m/lastIncludedIndex          -> uint64 big-endian (snapshot boundary)
//	m/lastIncludedTerm           -> uint64 big-endian (snapshot boundary)
var (
	logPrefix            = []byte("l/")
	keyTerm              = []byte("m/currentTerm")
	keyVotedFor          = []byte("m/votedFor")
	keyLastIncludedIndex = []byte("m/lastIncludedIndex")
	keyLastIncludedTerm  = []byte("m/lastIncludedTerm")
)

func logKey(index uint64) []byte {
	k := make([]byte, len(logPrefix)+8)
	copy(k, logPrefix)
	binary.BigEndian.PutUint64(k[len(logPrefix):], index)
	return k
}

func indexFromLogKey(k []byte) uint64 {
	return binary.BigEndian.Uint64(k[len(logPrefix):])
}

// PersistentLog manages the durable Raft log plus Raft's persistent
// metadata (currentTerm, votedFor) and the snapshot boundary
// (lastIncludedIndex/Term), all in the same storage engine and, where
// required for correctness, the same atomic batch.
//
// It keeps a small in-memory index of {firstIndex, lastIndex} so hot-path
// checks (log matching, sending AppendEntries) don't need a storage read,
// but it deliberately does NOT cache entries themselves indefinitely -
// only the most recently appended tail is memoized, per requirement 6
// ("avoid holding the entire Raft log in memory indefinitely").
type PersistentLog struct {
	mu sync.RWMutex

	engine storage.Engine

	// firstIndex/lastIndex describe the range of entries currently retained
	// on disk (after any compaction). firstIndex == lastIncludedIndex+1 when
	// the log is non-empty; if the log is empty, firstIndex == lastIndex+1.
	firstIndex uint64
	lastIndex  uint64

	lastIncludedIndex uint64
	lastIncludedTerm  uint64

	currentTerm uint64
	votedFor    string

	// tailCache holds a small, bounded number of the most recently appended
	// entries to serve fast-path replication reads without hitting disk.
	tailCache      map[uint64]Entry
	tailCacheLimit int
}

// OpenPersistentLog loads (or initializes) Raft persistent state from disk.
func OpenPersistentLog(engine storage.Engine) (*PersistentLog, error) {
	pl := &PersistentLog{
		engine:         engine,
		tailCache:      make(map[uint64]Entry),
		tailCacheLimit: 4096,
	}

	if v, err := engine.Get(keyTerm); err == nil {
		pl.currentTerm = binary.BigEndian.Uint64(v)
	} else if err != storage.ErrNotFound {
		return nil, err
	}

	if v, err := engine.Get(keyVotedFor); err == nil {
		pl.votedFor = string(v)
	} else if err != storage.ErrNotFound {
		return nil, err
	}

	if v, err := engine.Get(keyLastIncludedIndex); err == nil {
		pl.lastIncludedIndex = binary.BigEndian.Uint64(v)
	} else if err != storage.ErrNotFound {
		return nil, err
	}

	if v, err := engine.Get(keyLastIncludedTerm); err == nil {
		pl.lastIncludedTerm = binary.BigEndian.Uint64(v)
	} else if err != storage.ErrNotFound {
		return nil, err
	}

	pl.firstIndex = pl.lastIncludedIndex + 1
	pl.lastIndex = pl.lastIncludedIndex // updated below if entries exist

	it, err := engine.NewIterator(logPrefix, prefixUpperBound(logPrefix))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.Valid() {
		idx := indexFromLogKey(it.Key())
		if idx > pl.lastIndex {
			pl.lastIndex = idx
		}
		it.Next()
	}

	return pl, nil
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
	return nil // prefix was all 0xFF, unbounded
}

// --- Metadata accessors ---

func (pl *PersistentLog) CurrentTerm() uint64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.currentTerm
}

func (pl *PersistentLog) VotedFor() string {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.votedFor
}

// SetTermAndVote durably persists currentTerm and votedFor together, since
// Raft requires both to survive a crash consistently (a stale vote for an
// old term is invalid, so they must never be torn).
func (pl *PersistentLog) SetTermAndVote(term uint64, votedFor string) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	b := pl.engine.NewBatch()
	termBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(termBuf, term)
	b.Set(keyTerm, termBuf)
	b.Set(keyVotedFor, []byte(votedFor))
	if err := b.Commit(storage.WriteOptions{Sync: true}); err != nil {
		return err
	}
	pl.currentTerm = term
	pl.votedFor = votedFor
	return nil
}

// --- Log accessors ---

// LastIndex returns the index of the last entry in the log (or
// lastIncludedIndex if the log is empty because everything was
// snapshotted).
func (pl *PersistentLog) LastIndex() uint64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.lastIndex
}

// LastTerm returns the term of the last entry (or lastIncludedTerm if the
// log is empty).
func (pl *PersistentLog) LastTerm() uint64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	if pl.lastIndex == pl.lastIncludedIndex {
		return pl.lastIncludedTerm
	}
	e, ok := pl.tailCache[pl.lastIndex]
	if ok {
		return e.Term
	}
	entry, err := pl.getLocked(pl.lastIndex)
	if err != nil {
		return 0
	}
	return entry.Term
}

func (pl *PersistentLog) FirstIndex() uint64 {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.firstIndex
}

func (pl *PersistentLog) LastIncludedIndexAndTerm() (uint64, uint64) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.lastIncludedIndex, pl.lastIncludedTerm
}

// TermAt returns the term of the entry at index, or (0, false) if index is
// out of range or was already compacted away (except exactly
// lastIncludedIndex, whose term is known from the snapshot marker).
func (pl *PersistentLog) TermAt(index uint64) (uint64, bool) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	return pl.termAtLocked(index)
}

func (pl *PersistentLog) termAtLocked(index uint64) (uint64, bool) {
	if index == pl.lastIncludedIndex {
		return pl.lastIncludedTerm, true
	}
	if index < pl.firstIndex || index > pl.lastIndex {
		return 0, false
	}
	if e, ok := pl.tailCache[index]; ok {
		return e.Term, true
	}
	entry, err := pl.getLocked(index)
	if err != nil {
		return 0, false
	}
	return entry.Term, true
}

// Get returns the entry at index. Returns an error if it has been
// compacted or does not exist.
func (pl *PersistentLog) Get(index uint64) (Entry, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	if e, ok := pl.tailCache[index]; ok {
		return e, nil
	}
	return pl.getLocked(index)
}

func (pl *PersistentLog) getLocked(index uint64) (Entry, error) {
	if index <= pl.lastIncludedIndex {
		return Entry{}, fmt.Errorf("raft: index %d already compacted (lastIncludedIndex=%d)", index, pl.lastIncludedIndex)
	}
	v, err := pl.engine.Get(logKey(index))
	if err != nil {
		return Entry{}, err
	}
	var e Entry
	if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&e); err != nil {
		return Entry{}, err
	}
	return e, nil
}

// Range returns entries [from, to] inclusive, for replication.
func (pl *PersistentLog) Range(from, to uint64) ([]Entry, error) {
	pl.mu.RLock()
	defer pl.mu.RUnlock()
	if from > to {
		return nil, nil
	}
	entries := make([]Entry, 0, to-from+1)
	for i := from; i <= to; i++ {
		if e, ok := pl.tailCache[i]; ok {
			entries = append(entries, e)
			continue
		}
		e, err := pl.getLocked(i)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// AppendEntries durably appends entries starting at entries[0].Index,
// truncating any conflicting existing suffix first per the Raft log
// matching property. Entries must be contiguous and sorted by index.
func (pl *PersistentLog) AppendEntries(entries []Entry, sync bool) error {
	if len(entries) == 0 {
		return nil
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()

	b := pl.engine.NewBatch()

	firstNewIndex := entries[0].Index
	// If we're overwriting a conflicting suffix, delete the old tail from
	// disk so it can't resurface after a later truncation/restart.
	if firstNewIndex <= pl.lastIndex {
		for i := firstNewIndex; i <= pl.lastIndex; i++ {
			b.Delete(logKey(i))
			delete(pl.tailCache, i)
		}
	}

	for _, e := range entries {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(e); err != nil {
			return err
		}
		b.Set(logKey(e.Index), buf.Bytes())
	}

	if err := b.Commit(storage.WriteOptions{Sync: sync}); err != nil {
		return err
	}

	for _, e := range entries {
		pl.tailCache[e.Index] = e
	}
	pl.evictTailCacheLocked()

	last := entries[len(entries)-1]
	pl.lastIndex = last.Index
	if pl.firstIndex > pl.lastIncludedIndex+1 {
		// shouldn't happen, defensive
		pl.firstIndex = pl.lastIncludedIndex + 1
	}
	if pl.firstIndex == 0 {
		pl.firstIndex = entries[0].Index
	}
	return nil
}

func (pl *PersistentLog) evictTailCacheLocked() {
	if len(pl.tailCache) <= pl.tailCacheLimit {
		return
	}
	// Simple eviction: drop entries far behind lastIndex. Good enough since
	// the cache exists only to speed up the hot replication tail, not to
	// serve as a source of truth.
	threshold := pl.lastIndex - uint64(pl.tailCacheLimit)
	for idx := range pl.tailCache {
		if idx < threshold {
			delete(pl.tailCache, idx)
		}
	}
}

// TruncateSuffix removes all entries with index >= from (used when a
// follower's log conflicts with the leader's).
func (pl *PersistentLog) TruncateSuffix(from uint64) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if from > pl.lastIndex {
		return nil
	}
	b := pl.engine.NewBatch()
	for i := from; i <= pl.lastIndex; i++ {
		b.Delete(logKey(i))
		delete(pl.tailCache, i)
	}
	if err := b.Commit(storage.WriteOptions{Sync: true}); err != nil {
		return err
	}
	if from == pl.firstIndex {
		pl.lastIndex = pl.lastIncludedIndex
	} else {
		pl.lastIndex = from - 1
	}
	return nil
}

// CompactPrefix drops all entries with index <= lastIncludedIndex and
// records the new snapshot boundary, atomically with the metadata update.
// Called after a snapshot has been safely persisted.
func (pl *PersistentLog) CompactPrefix(lastIncludedIndex, lastIncludedTerm uint64) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if lastIncludedIndex <= pl.lastIncludedIndex {
		return nil // already compacted at least this far
	}

	b := pl.engine.NewBatch()
	end := lastIncludedIndex
	if end > pl.lastIndex {
		end = pl.lastIndex
	}
	for i := pl.firstIndex; i <= end; i++ {
		b.Delete(logKey(i))
		delete(pl.tailCache, i)
	}
	liBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(liBuf, lastIncludedIndex)
	ltBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ltBuf, lastIncludedTerm)
	b.Set(keyLastIncludedIndex, liBuf)
	b.Set(keyLastIncludedTerm, ltBuf)

	if err := b.Commit(storage.WriteOptions{Sync: true}); err != nil {
		return err
	}

	pl.lastIncludedIndex = lastIncludedIndex
	pl.lastIncludedTerm = lastIncludedTerm
	pl.firstIndex = lastIncludedIndex + 1
	if pl.lastIndex < lastIncludedIndex {
		pl.lastIndex = lastIncludedIndex
	}
	return nil
}

// RestoreSnapshotBoundary is used when installing a snapshot received from
// a leader (rather than one generated locally): it discards the entire
// local log (it cannot be trusted to be consistent with a snapshot that
// jumps ahead of it) and resets the boundary.
func (pl *PersistentLog) RestoreSnapshotBoundary(lastIncludedIndex, lastIncludedTerm uint64) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	b := pl.engine.NewBatch()
	for i := pl.firstIndex; i <= pl.lastIndex; i++ {
		b.Delete(logKey(i))
	}
	pl.tailCache = make(map[uint64]Entry)

	liBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(liBuf, lastIncludedIndex)
	ltBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ltBuf, lastIncludedTerm)
	b.Set(keyLastIncludedIndex, liBuf)
	b.Set(keyLastIncludedTerm, ltBuf)

	if err := b.Commit(storage.WriteOptions{Sync: true}); err != nil {
		return err
	}

	pl.lastIncludedIndex = lastIncludedIndex
	pl.lastIncludedTerm = lastIncludedTerm
	pl.firstIndex = lastIncludedIndex + 1
	pl.lastIndex = lastIncludedIndex
	return nil
}
