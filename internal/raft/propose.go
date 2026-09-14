package raft

import (
	"context"
	"errors"
)

// ErrNotLeader is returned by Propose when this node is not currently the
// leader. Callers (the server layer) translate this into a NOT_LEADER
// client response with LeaderHint().
var ErrNotLeader = errors.New("raft: not leader")

// ErrProposalDropped is returned when an in-flight proposal's log slot was
// overwritten (e.g. this node lost leadership and a new leader truncated
// the entry before it committed). The client should retry.
var ErrProposalDropped = errors.New("raft: proposal dropped, retry")

// Propose appends a client operation to the leader's log and blocks until
// it has been committed by a majority AND applied to the local state
// machine, which is the point at which this implementation considers a
// write durable and visible - matching the required PUT/DELETE flow:
// leader receives -> appends -> replicates -> majority commits -> applies
// -> returns success.
func (n *Node) Propose(ctx context.Context, entryType EntryType, key string, value []byte, clientRequestID string) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	entry := Entry{
		Type:            entryType,
		Key:             key,
		Value:           value,
		ClientRequestID: clientRequestID,
	}
	entry.Term = n.log.CurrentTerm()
	entry.Index = n.log.LastIndex() + 1

	waitCh := make(chan error, 1)
	n.writeWaiters[entry.Index] = waitCh

	// Index allocation and the corresponding append must be one atomic leader
	// operation. If we unlock before AppendEntries, two concurrent Propose calls
	// can both observe the same LastIndex and allocate the same log index, causing
	// one write to overwrite the other. Keep n.mu held until this entry is safely
	// appended so the next proposer necessarily observes the new LastIndex.
	if err := n.log.AppendEntries([]Entry{entry}, n.opts.FsyncOnAppend); err != nil {
		delete(n.writeWaiters, entry.Index)
		n.mu.Unlock()
		return err
	}
	n.mu.Unlock()

	// Replicate immediately rather than waiting for the next heartbeat tick.
	// Keep this call synchronous so Node.Stop() cannot return while an
	// untracked replication goroutine is still using the log/storage. The
	// per-peer RPCs inside broadcastAppendEntries still run concurrently.
	n.broadcastAppendEntries(ctx)

	select {
	case err := <-waitCh:
		return err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.writeWaiters, entry.Index)
		n.mu.Unlock()
		return ctx.Err()
	case <-n.stopCh:
		n.mu.Lock()
		delete(n.writeWaiters, entry.Index)
		n.mu.Unlock()
		return errors.New("raft: node stopping")
	}
}

// notifyWritersLocked is called from the apply loop (holding n.mu) each
// time an entry is applied, to unblock any Propose call waiting on that
// index. Must be called with n.mu held.
func (n *Node) notifyWritersLocked(appliedIndex uint64) {
	if ch, ok := n.writeWaiters[appliedIndex]; ok {
		ch <- nil
		delete(n.writeWaiters, appliedIndex)
	}
}

// failStaleWritersLocked is invoked when this node steps down from
// leadership (or discovers its proposed entry was truncated by a new
// leader) so callers blocked in Propose don't hang forever. Must be called
// with n.mu held.
func (n *Node) failStaleWritersLocked() {
	for idx, ch := range n.writeWaiters {
		ch <- ErrProposalDropped
		delete(n.writeWaiters, idx)
	}
}
