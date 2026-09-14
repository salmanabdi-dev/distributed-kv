package raft

import (
	"context"
	"log"
	"time"
)

// Start launches the node's background goroutines: the election/heartbeat
// timer loop and the apply loop. Safe to call once per Node.
func (n *Node) Start() {
	n.wg.Add(2)
	go n.runTimerLoop()
	go n.runApplyLoop()
}

// Stop signals all background goroutines to exit and waits for them. Safe
// to call more than once (including concurrently, e.g. from a test
// harness's explicit crash-simulation step and its deferred cleanup) -
// only the first call actually closes stopCh/waits; subsequent calls are
// no-ops. This is required because closing an already-closed channel
// panics, and Stop() has no other way to detect "already stopped" without
// this guard.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.wg.Wait()
	})
}

// runTimerLoop owns the single source of truth for "has an election
// timeout elapsed" and, while leader, "is it time to send heartbeats". It
// intentionally runs on one goroutine so there is exactly one timer to
// reason about instead of racing multiple timer goroutines against role
// changes.
func (n *Node) runTimerLoop() {
	defer n.wg.Done()

	timeout := n.opts.ElectionTimeoutMin + n.rnd.electionTimeout(0, n.opts.ElectionTimeoutMax-n.opts.ElectionTimeoutMin)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	heartbeat := time.NewTicker(n.opts.HeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-n.stopCh:
			return

		case <-n.electionResetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.opts.ElectionTimeoutMin + n.rnd.electionTimeout(0, n.opts.ElectionTimeoutMax-n.opts.ElectionTimeoutMin))

		case <-timer.C:
			if n.Role() != Leader {
				n.startElection()
			}
			timer.Reset(n.opts.ElectionTimeoutMin + n.rnd.electionTimeout(0, n.opts.ElectionTimeoutMax-n.opts.ElectionTimeoutMin))

		case <-heartbeat.C:
			if n.Role() == Leader {
				n.broadcastAppendEntries(context.Background())
			}
		}
	}
}

// resetElectionTimer is called whenever the node observes valid leader
// activity (AppendEntries/InstallSnapshot from the current term's leader)
// or grants a vote, per the Raft rules for suppressing unnecessary
// elections.
func (n *Node) resetElectionTimer() {
	select {
	case n.electionResetCh <- struct{}{}:
	default:
	}
}

func (n *Node) becomeFollower(term uint64, leaderID, leaderClientAddr string) {
	n.mu.Lock()
	changed := n.role != Follower
	wasLeader := n.role == Leader
	n.role = Follower
	n.leaderID = leaderID
	n.leaderClientAddr = leaderClientAddr
	if wasLeader {
		n.failStaleWritersLocked()
	}
	n.mu.Unlock()
	if term > n.log.CurrentTerm() {
		_ = n.log.SetTermAndVote(term, "")
	}
	if changed {
		log.Printf("[%s] became follower in term %d (leader=%s)", n.id, term, leaderID)
	}
}

func (n *Node) becomeCandidate() uint64 {
	newTerm := n.log.CurrentTerm() + 1
	_ = n.log.SetTermAndVote(newTerm, n.id)
	n.mu.Lock()
	n.role = Candidate
	n.leaderID = ""
	n.leaderClientAddr = ""
	n.mu.Unlock()
	log.Printf("[%s] became candidate for term %d", n.id, newTerm)
	return newTerm
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	if n.role != Candidate {
		n.mu.Unlock()
		return
	}
	n.role = Leader
	n.leaderID = n.id
	n.leaderClientAddr = n.selfClientAddr
	last := n.log.LastIndex()
	for _, p := range n.peers {
		n.nextIndex[p.ID] = last + 1
		n.matchIndex[p.ID] = 0
	}
	n.mu.Unlock()

	log.Printf("[%s] became leader for term %d", n.id, n.log.CurrentTerm())

	// Commit a no-op entry immediately so the leader can establish
	// commitIndex visibility into the current term without waiting for a
	// client write (needed for the ReadIndex safety argument: a leader may
	// not know which of its own previous-term entries are committed until
	// it has committed something in its own term).
	n.appendLocal(Entry{Type: EntryNoop})

	// Kick off replication immediately rather than waiting for the next
	// heartbeat tick, so followers learn about the new leader promptly.
	n.broadcastAppendEntries(context.Background())
}

// runApplyLoop applies committed-but-not-yet-applied entries to the state
// machine in strict index order, then advances lastApplied and satisfies
// any pending ReadIndex waiters and snapshot triggers.
func (n *Node) runApplyLoop() {
	defer n.wg.Done()
	// The ticker is a safety-net fallback (covers any missed signal, e.g.
	// a burst of signals collapsed by the non-blocking send below), not
	// the primary trigger - the primary trigger is commitAdvancedCh, so
	// applies happen immediately after commit rather than waiting out a
	// poll interval.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-n.commitAdvancedCh:
			n.applyCommitted()
		case <-ticker.C:
			n.applyCommitted()
		}
	}
}

// signalCommitAdvanced wakes the apply loop. Non-blocking: if a signal is
// already pending, this is a no-op, since applyCommitted always drains
// everything committed so far in one pass regardless of how many signals
// coalesced into that one wakeup.
func (n *Node) signalCommitAdvanced() {
	select {
	case n.commitAdvancedCh <- struct{}{}:
	default:
	}
}

func (n *Node) applyCommitted() {
	n.mu.Lock()
	commit := n.commitIndex
	applied := n.lastApplied
	n.mu.Unlock()

	for applied < commit {
		applied++
		entry, err := n.log.Get(applied)
		if err != nil {
			log.Printf("[%s] apply: failed to read committed entry %d: %v", n.id, applied, err)
			return
		}
		switch entry.Type {
		case EntryPut:
			if err := n.sm.ApplyPut(entry.Index, entry.Key, entry.Value); err != nil {
				log.Printf("[%s] apply put %d failed: %v", n.id, entry.Index, err)
				return
			}
		case EntryDelete:
			if err := n.sm.ApplyDelete(entry.Index, entry.Key); err != nil {
				log.Printf("[%s] apply delete %d failed: %v", n.id, entry.Index, err)
				return
			}
		case EntryNoop:
			n.sm.ApplyNoop(entry.Index)
		}

		n.mu.Lock()
		n.lastApplied = applied
		n.notifyWritersLocked(applied)
		n.satisfyPendingReadsLocked()
		n.mu.Unlock()

		n.mu.RLock()
		threshold := n.opts.SnapshotThreshold
		n.mu.RUnlock()
		if threshold > 0 && applied%threshold == 0 {
			// Snapshot synchronously so Stop() waiting for the apply loop is a
			// true quiescence point before the storage engines are closed.
			n.maybeSnapshot(applied)
		}
	}
}
