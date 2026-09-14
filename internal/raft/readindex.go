package raft

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrReadIndexTimeout is returned when leadership could not be confirmed
// by a majority within the read's context deadline.
var ErrReadIndexTimeout = errors.New("raft: readindex confirmation timed out")

// LinearizableRead implements the ReadIndex protocol (see design notes in
// README's "Linearizable Reads" section):
//
//  1. Record the current commitIndex as readIndex.
//  2. Confirm leadership by exchanging a heartbeat round with a majority
//     of peers *for the current term* - if this node is still leader in
//     that term after a majority responds, no other node could have been
//     elected leader more recently, so it is safe to answer with local
//     state.
//  3. Wait until lastApplied >= readIndex, so any write committed before
//     the read started is guaranteed visible.
//
// Returns once it is safe to read local state machine state for the
// caller; the caller (server layer) then performs the actual Get.
func (n *Node) LinearizableRead(ctx context.Context) error {
	n.mu.RLock()
	if n.role != Leader {
		n.mu.RUnlock()
		return ErrNotLeader
	}
	readIndex := n.commitIndex
	term := n.log.CurrentTerm()
	n.mu.RUnlock()

	if err := n.confirmLeadership(ctx, term); err != nil {
		return err
	}

	return n.waitForApplied(ctx, readIndex)
}

// confirmLeadership sends a heartbeat-equivalent AppendEntries to every
// peer and blocks until a majority (including self) has responded
// successfully in the same term, or ctx is done.
func (n *Node) confirmLeadership(ctx context.Context, term uint64) error {
	type result struct{ ok bool }

	// Use a child context so once a majority is reached (or the call fails),
	// outstanding heartbeat RPCs are cancelled and joined before returning.
	roundCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan result, len(n.peers))
	var wg sync.WaitGroup
	for _, p := range n.peers {
		peerID := p.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.mu.RLock()
			nextIdx := n.nextIndex[peerID]
			n.mu.RUnlock()
			prevLogIndex := uint64(0)
			if nextIdx > 0 {
				prevLogIndex = nextIdx - 1
			}
			prevLogTerm, _ := n.log.TermAt(prevLogIndex)

			resp, err := n.transport.AppendEntries(roundCtx, peerID, &AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: n.CommitIndex(),
			})
			if err != nil {
				results <- result{ok: false}
				return
			}
			if resp.Term > term {
				n.becomeFollower(resp.Term, "", "")
				results <- result{ok: false}
				return
			}
			// For ReadIndex confirmation we only need proof this peer still
			// recognizes us as leader in this term; a log mismatch does not
			// invalidate that leadership acknowledgement.
			results <- result{ok: true}
		}()
	}

	confirmed := 1 // self
	needed := (len(n.peers)+1)/2 + 1
	responded := 0
	var retErr error
	for responded < len(n.peers) && confirmed < needed {
		select {
		case r := <-results:
			responded++
			if r.ok {
				confirmed++
			}
		case <-ctx.Done():
			retErr = ctx.Err()
			cancel()
			responded = len(n.peers)
		case <-n.stopCh:
			retErr = errors.New("raft: node stopping")
			cancel()
			responded = len(n.peers)
		}
	}

	cancel()
	wg.Wait()

	if retErr != nil {
		return retErr
	}
	if confirmed >= needed {
		return nil
	}
	return ErrReadIndexTimeout
}

func (n *Node) waitForApplied(ctx context.Context, index uint64) error {
	n.mu.Lock()
	if n.lastApplied >= index {
		n.mu.Unlock()
		return nil
	}
	done := make(chan struct{})
	n.pendingReads = append(n.pendingReads, pendingRead{readIndex: index, done: done})
	n.mu.Unlock()

	removePending := func() {
		n.mu.Lock()
		for i, pr := range n.pendingReads {
			if pr.done == done {
				n.pendingReads = append(n.pendingReads[:i], n.pendingReads[i+1:]...)
				break
			}
		}
		n.mu.Unlock()
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		removePending()
		return ctx.Err()
	case <-n.stopCh:
		removePending()
		return errors.New("raft: node stopping")
	case <-time.After(5 * time.Second):
		removePending()
		return ErrReadIndexTimeout
	}
}

// satisfyPendingReadsLocked wakes any ReadIndex waiters whose target index
// has now been applied. Must be called with n.mu held.
func (n *Node) satisfyPendingReadsLocked() {
	if len(n.pendingReads) == 0 {
		return
	}
	remaining := n.pendingReads[:0]
	for _, pr := range n.pendingReads {
		if n.lastApplied >= pr.readIndex {
			close(pr.done)
		} else {
			remaining = append(remaining, pr)
		}
	}
	n.pendingReads = remaining
}
