package tests

import (
	"fmt"
	"testing"
	"time"
)

// TestSnapshotInstallationRecoversFarBehindFollower verifies requirement
// 7/8: a follower that misses so many writes that the leader has already
// compacted the log entries it would need can still catch up, via
// InstallSnapshot, rather than being stuck forever.
func TestSnapshotInstallationRecoversFarBehindFollower(t *testing.T) {
	const snapshotThreshold = 20
	h := newHarness(t, []string{"n1", "n2", "n3"}, snapshotThreshold)

	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	var followerID string
	for id := range h.nodes {
		if id != leader.id {
			followerID = id
			break
		}
	}
	h.stopNode(followerID)

	// Write enough entries, with the follower down the whole time, that
	// the leader snapshots and compacts its log well past what the
	// follower has seen - normal AppendEntries replication for those
	// indexes is no longer possible; only InstallSnapshot can catch it up.
	numWrites := snapshotThreshold * 5
	for i := 0; i < numWrites; i++ {
		h.put(t, leader, fmt.Sprintf("snap-key-%d", i), fmt.Sprintf("val-%d", i))
	}

	// Give the leader's async snapshot goroutine time to run and compact.
	time.Sleep(500 * time.Millisecond)

	h.restartNode(followerID, snapshotThreshold)

	deadline := time.Now().Add(5 * time.Second)
	lastKey := fmt.Sprintf("snap-key-%d", numWrites-1)
	for {
		resp := h.getRaw(t, h.nodes[followerID], lastKey, false)
		if string(resp.Value) == fmt.Sprintf("val-%d", numWrites-1) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never recovered via InstallSnapshot; last response: %+v", resp)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Spot-check an early key too, to confirm the full snapshot (not just
	// the tail) was installed correctly.
	resp := h.getRaw(t, h.nodes[followerID], "snap-key-0", false)
	if string(resp.Value) != "val-0" {
		t.Fatalf("early snapshot key missing/incorrect after InstallSnapshot: %+v", resp)
	}
}

// TestSnapshotRestoreOnOwnRestart verifies a node that snapshots and then
// restarts itself (not via InstallSnapshot from a peer, but loading its
// own local snapshot file) recovers correctly.
func TestSnapshotRestoreOnOwnRestart(t *testing.T) {
	const snapshotThreshold = 15
	h := newHarness(t, []string{"n1", "n2", "n3"}, snapshotThreshold)

	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	numWrites := snapshotThreshold * 3
	for i := 0; i < numWrites; i++ {
		h.put(t, leader, fmt.Sprintf("local-%d", i), fmt.Sprintf("v%d", i))
	}
	time.Sleep(500 * time.Millisecond) // let snapshot+compaction run

	leaderID := leader.id
	h.stopNode(leaderID)
	h.restartNode(leaderID, snapshotThreshold)

	deadline := time.Now().Add(3 * time.Second)
	for {
		resp := h.getRaw(t, h.nodes[leaderID], "local-0", false)
		if string(resp.Value) == "v0" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node did not restore its own local snapshot on restart")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
