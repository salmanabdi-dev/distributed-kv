package tests

import (
	"testing"
	"time"

	"github.com/salmanabdi-dev/distributed-kv/proto/kvpb"
)

func TestThreeNodeStartupElectsLeader(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected within 3s")
	}
}

func TestReplicatedPut(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	h.put(t, leader, "hello", "world")

	// A linearizable read from the leader must see it immediately.
	v, ok := h.get(t, leader, "hello", true)
	if !ok || v != "world" {
		t.Fatalf("get after put = (%q, %v), want (world, true)", v, ok)
	}
}

func TestReplicatedDelete(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	h.put(t, leader, "temp", "value")
	if v, ok := h.get(t, leader, "temp", true); !ok || v != "value" {
		t.Fatalf("expected temp=value before delete, got (%q,%v)", v, ok)
	}

	h.delete(t, leader, "temp")

	if _, ok := h.get(t, leader, "temp", true); ok {
		t.Fatalf("expected temp to be gone after delete")
	}
}

// TestFollowerRejectsLinearizableRead asserts the linearizability
// enforcement mechanism itself: a follower cannot serve a linearizable
// read locally (only the leader can, after ReadIndex confirmation), so it
// must respond NOT_LEADER rather than silently answering with
// possibly-stale local state.
func TestFollowerRejectsLinearizableRead(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	h.put(t, leader, "k", "v")

	found := false
	for id, n := range h.nodes {
		if id == leader.id {
			continue
		}
		resp := h.getRaw(t, n, "k", true)
		if resp.Status == kvpb.StatusCode_NOT_LEADER {
			found = true
		}
	}
	if !found {
		t.Fatal("expected at least one follower to reject a linearizable read with NOT_LEADER")
	}
}

// TestNonLinearizableReadOnFollowerEventuallyConsistent documents the
// explicitly weaker, opt-in local-read mode: it may lag but must
// eventually reflect a committed write.
func TestNonLinearizableReadOnFollowerEventuallyConsistent(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	h.put(t, leader, "eventual", "value")

	var follower *testNode
	for id, n := range h.nodes {
		if id != leader.id {
			follower = n
			break
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp := h.getRaw(t, follower, "eventual", false)
		if resp.Status == kvpb.StatusCode_OK && string(resp.Value) == "value" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never converged to committed value")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
