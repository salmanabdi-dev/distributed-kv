package raft

import (
	"context"
	"sync"
)

// startElection runs one Raft election attempt: increments term, votes for
// self, requests votes from all peers in parallel, and becomes leader if a
// majority (including self) grants a vote before the election attempt times
// out. The function does not return until all vote RPC goroutines have
// exited, which gives Node.Stop() a clean shutdown boundary and prevents
// background election work from touching storage after the caller closes it.
func (n *Node) startElection() {
	term := n.becomeCandidate()
	n.resetElectionTimer()

	lastIndex := n.log.LastIndex()
	lastTerm := n.log.LastTerm()

	total := len(n.peers) + 1
	needed := total/2 + 1
	if needed == 1 {
		n.becomeLeader()
		return
	}

	type voteResult struct {
		granted    bool
		higherTerm uint64
	}

	ctx, cancel := context.WithTimeout(context.Background(), n.opts.ElectionTimeoutMax)
	defer cancel()

	results := make(chan voteResult, len(n.peers))
	var wg sync.WaitGroup
	for _, p := range n.peers {
		peerID := p.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			rpcCtx, rpcCancel := context.WithTimeout(ctx, n.opts.ElectionTimeoutMin)
			defer rpcCancel()

			resp, err := n.transport.RequestVote(rpcCtx, peerID, &RequestVoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				results <- voteResult{}
				return
			}
			if resp.Term > term {
				results <- voteResult{higherTerm: resp.Term}
				return
			}
			results <- voteResult{granted: resp.VoteGranted}
		}()
	}

	votes := 1 // self vote
	responses := 0
	won := false

	for responses < len(n.peers) && !won {
		select {
		case r := <-results:
			responses++
			if r.higherTerm > term {
				n.becomeFollower(r.higherTerm, "", "")
				cancel()
				responses = len(n.peers)
				break
			}
			if r.granted {
				votes++
				if votes >= needed && n.Role() == Candidate && n.log.CurrentTerm() == term {
					won = true
					cancel()
				}
			}
		case <-ctx.Done():
			responses = len(n.peers)
		case <-n.stopCh:
			cancel()
			responses = len(n.peers)
		}
	}

	// Ensure every RequestVote goroutine has observed cancellation/returned
	// before startElection returns. This is important because Stop waits for
	// the timer loop, and the storage engine may be closed immediately after
	// Stop returns.
	wg.Wait()

	if won && n.Role() == Candidate && n.log.CurrentTerm() == term {
		n.becomeLeader()
	}
}

// HandleRequestVote implements the RequestVote RPC handler (Raft paper
// §5.2, §5.4.1). Called by the server layer after translating from the
// generated protobuf type.
func (n *Node) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	currentTerm := n.log.CurrentTerm()

	if req.Term < currentTerm {
		return &RequestVoteResponse{Term: currentTerm, VoteGranted: false}
	}

	if req.Term > currentTerm {
		// Discover higher term: step down and clear our vote before
		// evaluating this request against the new term.
		if n.role == Leader {
			n.failStaleWritersLocked()
		}
		n.role = Follower
		n.leaderID = ""
		n.leaderClientAddr = ""
		_ = n.log.SetTermAndVote(req.Term, "")
		currentTerm = req.Term
	}

	votedFor := n.log.VotedFor()
	logOK := n.candidateLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm)

	if (votedFor == "" || votedFor == req.CandidateID) && logOK {
		_ = n.log.SetTermAndVote(currentTerm, req.CandidateID)
		n.unlockedResetElectionTimer()
		return &RequestVoteResponse{Term: currentTerm, VoteGranted: true}
	}

	return &RequestVoteResponse{Term: currentTerm, VoteGranted: false}
}

// candidateLogUpToDateLocked implements the Raft "up-to-date" comparison:
// a candidate's log is at least as up-to-date as the voter's if it has a
// later last term, or the same last term and an equal-or-longer log.
func (n *Node) candidateLogUpToDateLocked(lastLogIndex, lastLogTerm uint64) bool {
	myLastIndex := n.log.LastIndex()
	myLastTerm := n.log.LastTerm()
	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

// unlockedResetElectionTimer is used from within functions that already
// hold n.mu, since resetElectionTimer itself doesn't need the lock but
// callers holding it must not deadlock on a re-entrant lock (Go mutexes
// aren't reentrant). It's a non-blocking channel send, so it's always
// lock-safe to call regardless.
func (n *Node) unlockedResetElectionTimer() {
	select {
	case n.electionResetCh <- struct{}{}:
	default:
	}
}
