// Package storage defines a small embedded key-value engine abstraction.
// Both the Raft log/metadata store and the replicated KV state machine sit
// on top of this interface, not on Pebble directly, so the engine could be
// swapped (e.g. for Badger) without touching raft or kv package internals.
package storage

import "errors"

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("storage: key not found")

// WriteOptions controls durability of a single write.
type WriteOptions struct {
	// Sync, if true, fsyncs the write-ahead log entry for this write before
	// returning. Required before a write may be treated as durable for
	// Raft persistence guarantees (currentTerm, votedFor, log entries).
	Sync bool
}

// Batch groups multiple writes into one atomic, optionally-synced commit.
// Used so a log append and metadata update can be committed together.
type Batch interface {
	Set(key, value []byte)
	Delete(key []byte)
	// Commit applies the batch atomically and, if opts.Sync, fsyncs it.
	Commit(opts WriteOptions) error
}

// Iterator walks a key range in ascending key order. Callers must call
// Close when done.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Close() error
}

// Engine is the minimal embedded-storage contract the rest of the system
// depends on.
type Engine interface {
	Get(key []byte) ([]byte, error) // returns ErrNotFound if absent
	Set(key, value []byte, opts WriteOptions) error
	Delete(key []byte, opts WriteOptions) error

	NewBatch() Batch

	// NewIterator returns an iterator over [lowerBound, upperBound).
	// A nil upperBound means "no upper bound".
	NewIterator(lowerBound, upperBound []byte) (Iterator, error)

	Close() error
}
