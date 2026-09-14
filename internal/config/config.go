// Package config loads node configuration from environment variables
// (with sane defaults), which is the primary mechanism used by the Docker
// Compose setup. A JSON file can optionally override/extend it for local,
// non-Docker runs.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Peer describes another node in the cluster as seen from this node.
type Peer struct {
	ID         string `json:"id"`
	RaftAddr   string `json:"raft_addr"`   // host:port for internal RaftService
	ClientAddr string `json:"client_addr"` // host:port for external KVService
}

// Config holds everything a node needs to start.
type Config struct {
	NodeID     string `json:"node_id"`
	RaftAddr   string `json:"raft_addr"`   // address this node's RaftService listens on
	ClientAddr string `json:"client_addr"` // address this node's KVService listens on
	Peers      []Peer `json:"peers"`       // all OTHER nodes in the cluster (not self)

	DataDir string `json:"data_dir"`

	// Election timeout is randomized in [ElectionTimeoutMin, ElectionTimeoutMax)
	// on every reset, per the Raft paper, to avoid split votes.
	ElectionTimeoutMin time.Duration `json:"election_timeout_min"`
	ElectionTimeoutMax time.Duration `json:"election_timeout_max"`
	HeartbeatInterval  time.Duration `json:"heartbeat_interval"`

	// SnapshotThreshold is the number of applied log entries since the last
	// snapshot after which a new snapshot is triggered.
	SnapshotThreshold uint64 `json:"snapshot_threshold"`

	// ReplicationBatchMaxEntries caps how many log entries are packed into a
	// single AppendEntries RPC when catching a follower up in bulk.
	ReplicationBatchMaxEntries int `json:"replication_batch_max_entries"`

	// FsyncOnAppend controls whether every persisted log append/metadata
	// write is fsync'd before being acknowledged as durable.
	//
	// true  (default): every write is fsync'd before the leader counts it
	//                   toward a majority. This is the durable, correct mode.
	// false           : writes are batched to the OS page cache and fsync'd
	//                   periodically by Pebble's own WAL sync policy instead
	//                   of on every single entry. This trades a window of
	//                   potential data loss on power-loss/OS-crash (not
	//                   process-crash) for higher throughput, and MUST be
	//                   called out explicitly in any benchmark that uses it.
	FsyncOnAppend bool `json:"fsync_on_append"`
}

func defaultConfig() Config {
	return Config{
		DataDir:                    "./data",
		ElectionTimeoutMin:         300 * time.Millisecond,
		ElectionTimeoutMax:         600 * time.Millisecond,
		HeartbeatInterval:          75 * time.Millisecond,
		SnapshotThreshold:          10000,
		ReplicationBatchMaxEntries: 512,
		FsyncOnAppend:              true,
	}
}

// Load builds a Config from (in increasing priority): built-in defaults,
// an optional JSON file (CONFIG_FILE env var), then individual environment
// variables. This lets docker-compose.yml set everything via env vars while
// still allowing a checked-in config file for local dev.
func Load() (Config, error) {
	cfg := defaultConfig()

	if path := os.Getenv("CONFIG_FILE"); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("reading config file %s: %w", path, err)
		}
		if err := json.Unmarshal(b, &cfg); err != nil {
			return cfg, fmt.Errorf("parsing config file %s: %w", path, err)
		}
	}

	if v := os.Getenv("NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := os.Getenv("RAFT_ADDR"); v != "" {
		cfg.RaftAddr = v
	}
	if v := os.Getenv("CLIENT_ADDR"); v != "" {
		cfg.ClientAddr = v
	}
	if v := os.Getenv("DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("PEERS"); v != "" {
		peers, err := parsePeers(v)
		if err != nil {
			return cfg, fmt.Errorf("parsing PEERS: %w", err)
		}
		cfg.Peers = peers
	}
	if v := os.Getenv("SNAPSHOT_THRESHOLD"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("parsing SNAPSHOT_THRESHOLD: %w", err)
		}
		cfg.SnapshotThreshold = n
	}
	if v := os.Getenv("REPLICATION_BATCH_MAX_ENTRIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("parsing REPLICATION_BATCH_MAX_ENTRIES: %w", err)
		}
		cfg.ReplicationBatchMaxEntries = n
	}
	if v := os.Getenv("ELECTION_TIMEOUT_MIN_MS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("parsing ELECTION_TIMEOUT_MIN_MS: %w", err)
		}
		cfg.ElectionTimeoutMin = time.Duration(n) * time.Millisecond
	}
	if v := os.Getenv("ELECTION_TIMEOUT_MAX_MS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("parsing ELECTION_TIMEOUT_MAX_MS: %w", err)
		}
		cfg.ElectionTimeoutMax = time.Duration(n) * time.Millisecond
	}
	if v := os.Getenv("HEARTBEAT_INTERVAL_MS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("parsing HEARTBEAT_INTERVAL_MS: %w", err)
		}
		cfg.HeartbeatInterval = time.Duration(n) * time.Millisecond
	}
	if v := os.Getenv("FSYNC_ON_APPEND"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return cfg, fmt.Errorf("parsing FSYNC_ON_APPEND: %w", err)
		}
		cfg.FsyncOnAppend = b
	}

	if cfg.NodeID == "" {
		return cfg, fmt.Errorf("NODE_ID is required")
	}
	if cfg.RaftAddr == "" {
		return cfg, fmt.Errorf("RAFT_ADDR is required")
	}
	if cfg.ClientAddr == "" {
		return cfg, fmt.Errorf("CLIENT_ADDR is required")
	}
	if cfg.ElectionTimeoutMin >= cfg.ElectionTimeoutMax {
		return cfg, fmt.Errorf("ELECTION_TIMEOUT_MIN_MS must be < ELECTION_TIMEOUT_MAX_MS")
	}

	return cfg, nil
}

// parsePeers parses "id1=raftAddr1|clientAddr1,id2=raftAddr2|clientAddr2".
func parsePeers(s string) ([]Peer, error) {
	var peers []Peer
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idAndAddrs := strings.SplitN(part, "=", 2)
		if len(idAndAddrs) != 2 {
			return nil, fmt.Errorf("invalid peer spec %q, want id=raftAddr|clientAddr", part)
		}
		addrs := strings.SplitN(idAndAddrs[1], "|", 2)
		if len(addrs) != 2 {
			return nil, fmt.Errorf("invalid peer spec %q, want id=raftAddr|clientAddr", part)
		}
		peers = append(peers, Peer{
			ID:         idAndAddrs[0],
			RaftAddr:   addrs[0],
			ClientAddr: addrs[1],
		})
	}
	return peers, nil
}
