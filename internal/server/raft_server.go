package server

import (
	"context"

	"github.com/salmanabdi-dev/distributed-kv/internal/raft"
	"github.com/salmanabdi-dev/distributed-kv/proto/raftpb"
)

// RaftServer implements raftpb.RaftServiceServer by delegating to a
// *raft.Node. It contains no consensus logic itself - purely translation
// at the RPC boundary, keeping internal/raft free of any gRPC dependency.
type RaftServer struct {
	raftpb.UnimplementedRaftServiceServer
	node *raft.Node
}

func NewRaftServer(node *raft.Node) *RaftServer {
	return &RaftServer{node: node}
}

func (s *RaftServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	resp := s.node.HandleRequestVote(&raft.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &raftpb.RequestVoteResponse{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	resp := s.node.HandleAppendEntries(&raft.AppendEntriesRequest{
		Term:             req.Term,
		LeaderID:         req.LeaderId,
		PrevLogIndex:     req.PrevLogIndex,
		PrevLogTerm:      req.PrevLogTerm,
		Entries:          fromPBEntries(req.Entries),
		LeaderCommit:     req.LeaderCommit,
		LeaderClientAddr: req.LeaderClientAddr,
	})
	return &raftpb.AppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictTerm:  resp.ConflictTerm,
		ConflictIndex: resp.ConflictIndex,
	}, nil
}

func (s *RaftServer) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	resp := s.node.HandleInstallSnapshot(&raft.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	return &raftpb.InstallSnapshotResponse{Term: resp.Term}, nil
}
