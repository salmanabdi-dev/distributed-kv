package raft

import (
	"testing"

	"github.com/salmanabdi-dev/distributed-kv/internal/storage"
)

// NOTE: these tests use a real Pebble-backed storage.Engine (via t.TempDir),
// not a mock, so they exercise the actual on-disk format. They have not
// been executed in the sandbox that generated this repository (no Go
// toolchain available there - see README). Run with:
//
//   go test ./internal/raft/...
//   go test -race ./internal/raft/...

func newTestLog(t *testing.T) *PersistentLog {
	t.Helper()
	eng, err := storage.OpenPebble(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	pl, err := OpenPersistentLog(eng)
	if err != nil {
		t.Fatalf("open persistent log: %v", err)
	}
	return pl
}

func TestPersistentLog_AppendAndGet(t *testing.T) {
	pl := newTestLog(t)

	entries := []Entry{
		{Index: 1, Term: 1, Type: EntryPut, Key: "a", Value: []byte("1")},
		{Index: 2, Term: 1, Type: EntryPut, Key: "b", Value: []byte("2")},
		{Index: 3, Term: 2, Type: EntryDelete, Key: "a"},
	}
	if err := pl.AppendEntries(entries, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	if got := pl.LastIndex(); got != 3 {
		t.Fatalf("LastIndex = %d, want 3", got)
	}
	if got := pl.LastTerm(); got != 2 {
		t.Fatalf("LastTerm = %d, want 2", got)
	}

	e, err := pl.Get(2)
	if err != nil {
		t.Fatalf("get(2): %v", err)
	}
	if e.Key != "b" || string(e.Value) != "2" {
		t.Fatalf("unexpected entry at 2: %+v", e)
	}
}

func TestPersistentLog_TruncateSuffixOnConflict(t *testing.T) {
	pl := newTestLog(t)

	_ = pl.AppendEntries([]Entry{
		{Index: 1, Term: 1, Type: EntryPut, Key: "a"},
		{Index: 2, Term: 1, Type: EntryPut, Key: "b"},
		{Index: 3, Term: 1, Type: EntryPut, Key: "c"},
	}, true)

	// Simulate a new leader overwriting entries 2-3 with a higher term.
	if err := pl.AppendEntries([]Entry{
		{Index: 2, Term: 2, Type: EntryPut, Key: "b2"},
	}, true); err != nil {
		t.Fatalf("append conflicting: %v", err)
	}

	if got := pl.LastIndex(); got != 2 {
		t.Fatalf("LastIndex after conflict-append = %d, want 2 (old index 3 should be gone)", got)
	}
	e, err := pl.Get(2)
	if err != nil {
		t.Fatalf("get(2): %v", err)
	}
	if e.Key != "b2" || e.Term != 2 {
		t.Fatalf("entry 2 not overwritten: %+v", e)
	}
	if _, err := pl.Get(3); err == nil {
		t.Fatalf("expected entry 3 to be gone after truncation")
	}
}

func TestPersistentLog_TermAndVotePersist(t *testing.T) {
	eng, err := storage.OpenPebble(t.TempDir())
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	defer eng.Close()

	pl, err := OpenPersistentLog(eng)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := pl.SetTermAndVote(5, "node-2"); err != nil {
		t.Fatalf("set term/vote: %v", err)
	}

	// Reopen against the SAME underlying engine to simulate a restart, and
	// confirm currentTerm/votedFor survived.
	pl2, err := OpenPersistentLog(eng)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if pl2.CurrentTerm() != 5 {
		t.Fatalf("CurrentTerm after reopen = %d, want 5", pl2.CurrentTerm())
	}
	if pl2.VotedFor() != "node-2" {
		t.Fatalf("VotedFor after reopen = %q, want node-2", pl2.VotedFor())
	}
}

func TestPersistentLog_CompactPrefix(t *testing.T) {
	pl := newTestLog(t)
	_ = pl.AppendEntries([]Entry{
		{Index: 1, Term: 1, Type: EntryPut, Key: "a"},
		{Index: 2, Term: 1, Type: EntryPut, Key: "b"},
		{Index: 3, Term: 1, Type: EntryPut, Key: "c"},
		{Index: 4, Term: 1, Type: EntryPut, Key: "d"},
	}, true)

	if err := pl.CompactPrefix(2, 1); err != nil {
		t.Fatalf("compact: %v", err)
	}

	if _, err := pl.Get(1); err == nil {
		t.Fatalf("expected index 1 to be compacted away")
	}
	if _, err := pl.Get(2); err == nil {
		t.Fatalf("expected index 2 (lastIncludedIndex) to be compacted away from the log itself")
	}
	e, err := pl.Get(3)
	if err != nil || e.Key != "c" {
		t.Fatalf("expected index 3 to remain, got %+v err=%v", e, err)
	}
	li, lt := pl.LastIncludedIndexAndTerm()
	if li != 2 || lt != 1 {
		t.Fatalf("LastIncludedIndexAndTerm = (%d,%d), want (2,1)", li, lt)
	}
}

func TestPersistentLog_TruncateSuffixFullClear(t *testing.T) {
	pl := newTestLog(t)
	_ = pl.AppendEntries([]Entry{
		{Index: 1, Term: 1, Key: "a", Type: EntryPut},
		{Index: 2, Term: 1, Key: "b", Type: EntryPut},
	}, true)

	if err := pl.TruncateSuffix(1); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := pl.LastIndex(); got != 0 {
		t.Fatalf("LastIndex after full truncate = %d, want 0", got)
	}
}
