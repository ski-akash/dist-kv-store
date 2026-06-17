package server

import (
	"context"

	"github.com/ski-akash/dist-kv-store/internal/storage"
	pb "github.com/ski-akash/dist-kv-store/proto"
)

// KVServer implements the generated gRPC interface
type KVServer struct {
	pb.UnimplementedKVStoreServer
	store *storage.KVStore
	wal   *storage.WAL
}

// NewKVServer initializes a new gRPC handler
func NewKVServer(store *storage.KVStore, wal *storage.WAL) *KVServer {
	return &KVServer{
		store: store,
		wal:   wal,
	}
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// 1. Write to disk FIRST for durability
	err := s.wal.Append(storage.LogEntry{
		Operation: "PUT",
		Key:       req.Key,
		Value:     req.Value,
	})
	if err != nil {
		return &pb.PutResponse{Success: false}, err
	}

	// 2. Write to memory
	s.store.Put(req.Key, req.Value)
	return &pb.PutResponse{Success: true}, nil
}

func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, found := s.store.Get(req.Key)
	return &pb.GetResponse{Value: value, Found: found}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	err := s.wal.Append(storage.LogEntry{
		Operation: "DELETE",
		Key:       req.Key,
	})
	if err != nil {
		return &pb.DeleteResponse{Success: false}, err
	}

	s.store.Delete(req.Key)
	return &pb.DeleteResponse{Success: true}, nil
}
