package raft

import (
	"hash/fnv"
	"math/rand"
	"time"
)

// randSource is a per-node PRNG for randomized election timeouts. Seeding
// from the node ID (rather than a single shared global seed) avoids all
// nodes started in the same millisecond drawing correlated timeouts, which
// would defeat the purpose of randomization and increase split-vote odds.
type randSource struct {
	r *rand.Rand
}

func newRandSource(nodeID string) *randSource {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeID))
	seed := int64(h.Sum64()) ^ time.Now().UnixNano()
	return &randSource{r: rand.New(rand.NewSource(seed))}
}

func (rs *randSource) electionTimeout(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	span := int64(max - min)
	return min + time.Duration(rs.r.Int63n(span))
}
