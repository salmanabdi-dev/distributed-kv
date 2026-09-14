package server

import (
	"context"
	"errors"

	"github.com/salmanabdi-dev/distributed-kv/internal/kv"
	"github.com/salmanabdi-dev/distributed-kv/internal/raft"
	"github.com/salmanabdi-dev/distributed-kv/proto/kvpb"
)

// ClientServer implements kvpb.KVServiceServer, the external client-facing
// API. It never forwards requests to the leader on the client's behalf;
// instead it returns NOT_LEADER with a best-effort leader hint, so clients
// (including the benchmark tool) retry directly against the leader. This
// avoids doubling network hops on every write in the common case where a
// client already knows/caches the leader.
type ClientServer struct {
	kvpb.UnimplementedKVServiceServer
	node *raft.Node
	sm   *kv.StateMachine
}

func NewClientServer(node *raft.Node, sm *kv.StateMachine) *ClientServer {
	return &ClientServer{node: node, sm: sm}
}

func (s *ClientServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	err := s.node.Propose(ctx, raft.EntryPut, req.Key, req.Value, req.ClientRequestId)
	return &kvpb.PutResponse{
		Status:     statusFor(err, s.node),
		LeaderHint: s.node.LeaderHint(),
		Error:      errString(err),
	}, nil
}

func (s *ClientServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	err := s.node.Propose(ctx, raft.EntryDelete, req.Key, nil, req.ClientRequestId)
	return &kvpb.DeleteResponse{
		Status:     statusFor(err, s.node),
		LeaderHint: s.node.LeaderHint(),
		Error:      errString(err),
	}, nil
}

func (s *ClientServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if req.Linearizable {
		if err := s.node.LinearizableRead(ctx); err != nil {
			return &kvpb.GetResponse{
				Status:     statusFor(err, s.node),
				LeaderHint: s.node.LeaderHint(),
				Error:      errString(err),
			}, nil
		}
	}

	value, found, err := s.sm.Get(req.Key)
	if err != nil {
		return &kvpb.GetResponse{Status: kvpb.StatusCode_INTERNAL_ERROR, Error: err.Error()}, nil
	}
	if !found {
		return &kvpb.GetResponse{Status: kvpb.StatusCode_NOT_FOUND}, nil
	}
	return &kvpb.GetResponse{Status: kvpb.StatusCode_OK, Value: value}, nil
}

func statusFor(err error, node *raft.Node) kvpb.StatusCode {
	switch {
	case err == nil:
		return kvpb.StatusCode_OK
	case errors.Is(err, raft.ErrNotLeader):
		return kvpb.StatusCode_NOT_LEADER
	case errors.Is(err, context.DeadlineExceeded):
		return kvpb.StatusCode_TIMEOUT
	case errors.Is(err, raft.ErrReadIndexTimeout):
		return kvpb.StatusCode_TIMEOUT
	case errors.Is(err, raft.ErrProposalDropped):
		return kvpb.StatusCode_NOT_LEADER
	default:
		return kvpb.StatusCode_INTERNAL_ERROR
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
