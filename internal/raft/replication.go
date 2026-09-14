package raft

import (
	"context"
	"sort"
	"sync"
)

// appendLocal is called only while holding leadership to append a new
// entry (client write or the term-start no-op) to the local log at the
// next available index/current term. It does not itself replicate; the
// caller (Propose, or becomeLeader for the no-op) triggers replication.
func (n *Node) appendLocal(e Entry) Entry {
	term := n.log.CurrentTerm()
	e.Term = term
	e.Index = n.log.LastIndex() + 1
	_ = n.log.AppendEntries([]Entry{e}, n.opts.FsyncOnAppend)
	return e
}

// broadcastAppendEntries sends AppendEntries (heartbeat or with new
// entries, whichever is appropriate per-follower) to every peer in
// parallel, then attempts to advance commitIndex based on the resulting
// matchIndex values. Safe to call frequently; each peer send is
// independent and non-blocking with respect to the others.
func (n *Node) broadcastAppendEntries(ctx context.Context) {
	n.replicationMu.Lock()
	defer n.replicationMu.Unlock()

	if !n.IsLeader() {
		return
	}
	term := n.log.CurrentTerm()

	var wg sync.WaitGroup
	for _, p := range n.peers {
		peerID := p.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.replicateToPeer(ctx, peerID, term)
		}()
	}
	wg.Wait()

	n.tryAdvanceCommitIndex(term)
}

// replicateToPeer sends exactly one AppendEntries RPC to peerID containing
// whatever entries that peer is currently missing (bounded by
// ReplicationBatchMaxEntries), or an empty heartbeat if it is caught up.
// On success it advances nextIndex/matchIndex; on log inconsistency it
// backs nextIndex up using the follower's conflict hint so catch-up after
// a partition takes O(1) round trips per differing term rather than O(n).
func (n *Node) replicateToPeer(ctx context.Context, peerID string, term uint64) {
	n.mu.RLock()
	if n.role != Leader || n.log.CurrentTerm() != term {
		n.mu.RUnlock()
		return
	}
	nextIdx := n.nextIndex[peerID]
	n.mu.RUnlock()

	firstAvailable := n.log.FirstIndex()
	if nextIdx < firstAvailable {
		// The follower needs entries that have already been compacted into
		// a snapshot; catch it up with InstallSnapshot instead.
		n.sendInstallSnapshot(ctx, peerID, term)
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm, ok := n.log.TermAt(prevLogIndex)
	if !ok && prevLogIndex != 0 {
		n.sendInstallSnapshot(ctx, peerID, term)
		return
	}

	lastIndex := n.log.LastIndex()
	entries := []Entry{}
	if lastIndex >= nextIdx {
		end := lastIndex
		maxBatch := uint64(n.opts.ReplicationBatchMaxEntries)
		if maxBatch > 0 && end-nextIdx+1 > maxBatch {
			end = nextIdx + maxBatch - 1
		}
		es, err := n.log.Range(nextIdx, end)
		if err != nil {
			// Entries were compacted concurrently; fall back to snapshot.
			n.sendInstallSnapshot(ctx, peerID, term)
			return
		}
		entries = es
	}

	n.mu.RLock()
	leaderClientAddr := n.selfClientAddr
	n.mu.RUnlock()

	resp, err := n.transport.AppendEntries(ctx, peerID, &AppendEntriesRequest{
		Term:             term,
		LeaderID:         n.id,
		PrevLogIndex:     prevLogIndex,
		PrevLogTerm:      prevLogTerm,
		Entries:          entries,
		LeaderCommit:     n.CommitIndex(),
		LeaderClientAddr: leaderClientAddr,
	})
	if err != nil {
		return // peer unreachable; will retry on next tick
	}

	if resp.Term > term {
		n.becomeFollower(resp.Term, "", "")
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.log.CurrentTerm() != term {
		return
	}

	if resp.Success {
		if len(entries) > 0 {
			n.matchIndex[peerID] = entries[len(entries)-1].Index
			n.nextIndex[peerID] = n.matchIndex[peerID] + 1
		}
		return
	}

	// Conflict: back nextIndex up using the fast-backtrack hint.
	if resp.ConflictTerm != 0 {
		// Find the last entry we have in ConflictTerm, if any.
		newNext := resp.ConflictIndex
		for idx := n.log.LastIndex(); idx >= n.log.FirstIndex() && idx > 0; idx-- {
			t, ok := n.log.TermAt(idx)
			if !ok {
				break
			}
			if t == resp.ConflictTerm {
				newNext = idx + 1
				break
			}
			if t < resp.ConflictTerm {
				break
			}
		}
		if newNext < 1 {
			newNext = 1
		}
		n.nextIndex[peerID] = newNext
	} else if resp.ConflictIndex > 0 {
		n.nextIndex[peerID] = resp.ConflictIndex
	} else if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
}

// tryAdvanceCommitIndex implements the Raft commitment rule: a log entry
// is committed once it is stored on a majority of servers AND at least one
// entry from the leader's current term has been replicated to a majority
// (the "leader completeness" safety check that prevents committing entries
// from prior terms based solely on a stale majority count).
func (n *Node) tryAdvanceCommitIndex(term uint64) {
	n.mu.Lock()

	if n.role != Leader || n.log.CurrentTerm() != term {
		n.mu.Unlock()
		return
	}

	matchIndexes := make([]uint64, 0, len(n.peers)+1)
	matchIndexes = append(matchIndexes, n.log.LastIndex()) // leader's own match
	for _, p := range n.peers {
		matchIndexes = append(matchIndexes, n.matchIndex[p.ID])
	}
	sort.Slice(matchIndexes, func(i, j int) bool { return matchIndexes[i] < matchIndexes[j] })

	// The median of sorted matchIndexes (including self) is the highest
	// index replicated to a majority.
	majorityIndex := matchIndexes[(len(matchIndexes)-1)/2]

	if majorityIndex <= n.commitIndex {
		n.mu.Unlock()
		return
	}
	entryTerm, ok := n.log.TermAt(majorityIndex)
	if !ok || entryTerm != term {
		// Cannot commit an entry from a previous term purely by count; it
		// will be committed implicitly once a current-term entry commits.
		n.mu.Unlock()
		return
	}

	n.commitIndex = majorityIndex
	n.mu.Unlock()
	n.signalCommitAdvanced()
}

// HandleAppendEntries implements the AppendEntries RPC handler (Raft paper
// §5.3). Called by the server layer after translating from protobuf.
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	n.mu.Lock()

	currentTerm := n.log.CurrentTerm()
	if req.Term < currentTerm {
		n.mu.Unlock()
		return &AppendEntriesResponse{Term: currentTerm, Success: false}
	}

	if req.Term > currentTerm {
		if n.role == Leader {
			n.failStaleWritersLocked()
		}
		n.role = Follower
		_ = n.log.SetTermAndVote(req.Term, "")
		currentTerm = req.Term
	} else if n.role == Candidate {
		n.role = Follower
	}

	n.leaderID = req.LeaderID
	n.leaderClientAddr = req.LeaderClientAddr
	n.mu.Unlock()
	n.resetElectionTimer()

	// Log consistency check.
	if req.PrevLogIndex > 0 {
		myTerm, ok := n.log.TermAt(req.PrevLogIndex)
		if !ok {
			// We don't have prevLogIndex at all (log too short, or it was
			// compacted away as part of a snapshot boundary mismatch).
			lastIdx := n.log.LastIndex()
			return &AppendEntriesResponse{
				Term:          currentTerm,
				Success:       false,
				ConflictIndex: lastIdx + 1,
			}
		}
		if myTerm != req.PrevLogTerm {
			conflictTerm := myTerm
			conflictIndex := req.PrevLogIndex
			for conflictIndex > n.log.FirstIndex() {
				t, ok := n.log.TermAt(conflictIndex - 1)
				if !ok || t != conflictTerm {
					break
				}
				conflictIndex--
			}
			return &AppendEntriesResponse{
				Term:          currentTerm,
				Success:       false,
				ConflictTerm:  conflictTerm,
				ConflictIndex: conflictIndex,
			}
		}
	}

	if len(req.Entries) > 0 {
		if err := n.log.AppendEntries(req.Entries, n.opts.FsyncOnAppend); err != nil {
			return &AppendEntriesResponse{Term: currentTerm, Success: false}
		}
	}

	if req.LeaderCommit > n.CommitIndex() {
		n.mu.Lock()
		newCommit := req.LeaderCommit
		lastNew := n.log.LastIndex()
		if newCommit > lastNew {
			newCommit = lastNew
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.mu.Unlock()
			n.signalCommitAdvanced()
		} else {
			n.mu.Unlock()
		}
	}

	return &AppendEntriesResponse{Term: currentTerm, Success: true}
}
