package tests

import (
	"testing"
	"time"
)

func TestFollowerCrashClusterStillAvailable(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
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

	// Cluster must still accept writes with only 2/3 nodes up (majority).
	h.put(t, leader, "still-available", "yes")
	if v, ok := h.get(t, leader, "still-available", true); !ok || v != "yes" {
		t.Fatalf("write not committed with a follower down: (%q,%v)", v, ok)
	}
}

func TestLeaderCrashNewLeaderElectedAndDataSurvives(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader1 := h.findLeader(3 * time.Second)
	if leader1 == nil {
		t.Fatal("no leader elected")
	}

	h.put(t, leader1, "before-crash", "durable")

	h.stopNode(leader1.id)

	var leader2 *testNode
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id, n := range h.nodes {
			if id != leader1.id && n.node.IsLeader() {
				leader2 = n
			}
		}
		if leader2 != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader2 == nil {
		t.Fatal("no new leader elected after leader crash")
	}

	// The committed write from before the crash must have survived on the
	// surviving majority and be visible via the new leader.
	if v, ok := h.get(t, leader2, "before-crash", true); !ok || v != "durable" {
		t.Fatalf("committed write lost after leader crash: (%q,%v)", v, ok)
	}

	h.put(t, leader2, "after-failover", "also-durable")
	if v, ok := h.get(t, leader2, "after-failover", true); !ok || v != "also-durable" {
		t.Fatalf("new leader cannot commit new writes: (%q,%v)", v, ok)
	}
}

func TestFollowerRestartRecoversPersistedStateAndCatchesUp(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	h.put(t, leader, "k1", "v1")

	var followerID string
	for id := range h.nodes {
		if id != leader.id {
			followerID = id
			break
		}
	}
	h.stopNode(followerID)

	// Write more while the follower is down, so it has catching up to do.
	h.put(t, leader, "k2", "v2")

	h.restartNode(followerID, 0)

	deadline := time.Now().Add(3 * time.Second)
	for {
		resp := h.getRaw(t, h.nodes[followerID], "k2", false)
		if string(resp.Value) == "v2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted follower never caught up on missed writes")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// k1, written before the crash, must also have survived the restart
	// via persisted Raft log / state machine storage.
	resp := h.getRaw(t, h.nodes[followerID], "k1", false)
	if string(resp.Value) != "v1" {
		t.Fatalf("restarted follower lost pre-crash committed data")
	}
}

func TestOldLeaderStepsDownAfterHigherTermDiscovered(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader1 := h.findLeader(3 * time.Second)
	if leader1 == nil {
		t.Fatal("no leader elected")
	}
	term1 := leader1.node.CurrentTerm()

	h.stopNode(leader1.id)
	h.restartNode(leader1.id, 0)

	// After rejoining, the old leader (now just a rejoined node) must not
	// remain "leader" in a stale term: it should observe the higher term
	// from the new leader's heartbeats and step down to follower.
	deadline := time.Now().Add(3 * time.Second)
	for {
		n := h.nodes[leader1.id]
		if n.node.CurrentTerm() > term1 && !n.node.IsLeader() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rejoined old leader did not step down / adopt higher term")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
