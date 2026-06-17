package main

import (
	"log"
	"net"

	"github.com/ski-akash/dist-kv-store/internal/server"
	"github.com/ski-akash/dist-kv-store/internal/storage"
	pb "github.com/ski-akash/dist-kv-store/proto"

	"google.golang.org/grpc"
)

func main() {
	// 1. Initialize the Write-Ahead Log
	wal, err := storage.NewWAL("kvstore.wal")
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// 2. Initialize the In-Memory Store
	store := storage.NewKVStore()

	// 3. Recover any existing data from a previous run
	if err := store.RecoverFromWAL(wal); err != nil {
		log.Fatalf("Failed to recover from WAL: %v", err)
	}
	log.Println("Database engine loaded successfully.")

	// 4. Set up the gRPC Server
	grpcServer := grpc.NewServer()
	kvServer := server.NewKVServer(store, wal)
	pb.RegisterKVStoreServer(grpcServer, kvServer)

	// 5. Start listening on a TCP port
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	log.Println("KV Store server is running on port 50051...")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
