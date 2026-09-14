// Command node runs a single distributed-kv cluster member: it opens local
// storage, constructs the Raft node, and serves both the RaftService
// (internal, node-to-node) and KVService (external, client-facing) gRPC
// APIs.
package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"

	"github.com/salmanabdi-dev/distributed-kv/internal/config"
	"github.com/salmanabdi-dev/distributed-kv/internal/kv"
	"github.com/salmanabdi-dev/distributed-kv/internal/raft"
	"github.com/salmanabdi-dev/distributed-kv/internal/server"
	"github.com/salmanabdi-dev/distributed-kv/internal/storage"
	"github.com/salmanabdi-dev/distributed-kv/proto/kvpb"
	"github.com/salmanabdi-dev/distributed-kv/proto/raftpb"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("starting node %s: raft=%s client=%s peers=%v data_dir=%s fsync=%v",
		cfg.NodeID, cfg.RaftAddr, cfg.ClientAddr, cfg.Peers, cfg.DataDir, cfg.FsyncOnAppend)

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		log.Fatalf("creating data dir: %v", err)
	}

	// Two separate embedded stores: one for the Raft log + metadata, one
	// for the committed KV data. Keeping them separate (rather than one
	// keyspace-shared Pebble instance) means state-machine snapshot/restore
	// and log compaction never risk touching each other's keys, and each
	// can be tuned/relocated independently later if needed.
	raftEngine, err := storage.OpenPebble(filepath.Join(cfg.DataDir, "raftlog"))
	if err != nil {
		log.Fatalf("opening raft log storage: %v", err)
	}
	kvEngine, err := storage.OpenPebble(filepath.Join(cfg.DataDir, "kvdata"))
	if err != nil {
		log.Fatalf("opening kv storage: %v", err)
	}

	snapStore, err := raft.NewSnapshotStore(filepath.Join(cfg.DataDir, "snapshots"))
	if err != nil {
		log.Fatalf("opening snapshot store: %v", err)
	}

	stateMachine := kv.NewStateMachine(kvEngine)

	peerRaftAddrs := make(map[string]string)
	var peers []raft.PeerInfo
	for _, p := range cfg.Peers {
		peerRaftAddrs[p.ID] = p.RaftAddr
		peers = append(peers, raft.PeerInfo{ID: p.ID})
	}
	transport := server.NewGRPCTransport(peerRaftAddrs)
	defer func() {
		if err := transport.Close(); err != nil {
			log.Printf("closing raft transport: %v", err)
		}
	}()

	node, err := raft.NewNode(
		cfg.NodeID,
		peers,
		raft.Options{
			ElectionTimeoutMin:         cfg.ElectionTimeoutMin,
			ElectionTimeoutMax:         cfg.ElectionTimeoutMax,
			HeartbeatInterval:          cfg.HeartbeatInterval,
			SnapshotThreshold:          cfg.SnapshotThreshold,
			ReplicationBatchMaxEntries: cfg.ReplicationBatchMaxEntries,
			FsyncOnAppend:              cfg.FsyncOnAppend,
		},
		transport,
		raftEngine,
		stateMachine,
		snapStore,
		cfg.ClientAddr,
	)
	if err != nil {
		log.Fatalf("constructing raft node: %v", err)
	}
	node.Start()

	raftLis, err := net.Listen("tcp", cfg.RaftAddr)
	if err != nil {
		log.Fatalf("listening on raft addr %s: %v", cfg.RaftAddr, err)
	}
	raftGRPC := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(raftGRPC, server.NewRaftServer(node))
	go func() {
		log.Printf("raft service listening on %s", cfg.RaftAddr)
		if err := raftGRPC.Serve(raftLis); err != nil {
			log.Fatalf("raft grpc server: %v", err)
		}
	}()

	clientLis, err := net.Listen("tcp", cfg.ClientAddr)
	if err != nil {
		log.Fatalf("listening on client addr %s: %v", cfg.ClientAddr, err)
	}
	clientGRPC := grpc.NewServer()
	kvpb.RegisterKVServiceServer(clientGRPC, server.NewClientServer(node, stateMachine))
	go func() {
		log.Printf("kv client service listening on %s", cfg.ClientAddr)
		if err := clientGRPC.Serve(clientLis); err != nil {
			log.Fatalf("client grpc server: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down node %s", cfg.NodeID)

	clientGRPC.GracefulStop()
	raftGRPC.GracefulStop()
	node.Stop()
	_ = raftEngine.Close()
	_ = kvEngine.Close()
}
