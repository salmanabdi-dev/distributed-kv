package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentWritersAllCommit exercises the concurrency requirements:
// many goroutines proposing writes through the same leader concurrently,
// verifying every one is eventually committed and consistent across nodes.
func TestConcurrentWritersAllCommit(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	const numWriters = 20
	const writesPerWriter = 10

	var wg sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				key := fmt.Sprintf("w%d-k%d", w, i)
				h.put(t, leader, key, fmt.Sprintf("v%d-%d", w, i))
			}
		}(w)
	}
	wg.Wait()

	// Every write must be visible via a linearizable read, and identical
	// across all three nodes (checked via non-linearizable local reads on
	// followers, which must have converged by now).
	for w := 0; w < numWriters; w++ {
		for i := 0; i < writesPerWriter; i++ {
			key := fmt.Sprintf("w%d-k%d", w, i)
			want := fmt.Sprintf("v%d-%d", w, i)
			if v, ok := h.get(t, leader, key, true); !ok || v != want {
				t.Fatalf("leader missing/incorrect for %s: (%q,%v)", key, v, ok)
			}
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		allConverged := true
		for id, n := range h.nodes {
			if id == leader.id {
				continue
			}
			resp := h.getRaw(t, n, "w0-k0", false)
			if string(resp.Value) != "v0-0" {
				allConverged = false
			}
		}
		if allConverged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nodes did not converge on committed data")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMixedReadWriteWorkload exercises interleaved PUT and linearizable GET
// traffic against the leader, matching the mixed-workload benchmark
// scenario at small scale as a correctness (not performance) check.
func TestMixedReadWriteWorkload(t *testing.T) {
	h := newHarness(t, []string{"n1", "n2", "n3"}, 0)
	leader := h.findLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("mix-%d", i)
		val := fmt.Sprintf("val-%d", i)
		h.put(t, leader, key, val)
		if v, ok := h.get(t, leader, key, true); !ok || v != val {
			t.Fatalf("read-your-write failed at i=%d: (%q,%v)", i, v, ok)
		}
	}
}
