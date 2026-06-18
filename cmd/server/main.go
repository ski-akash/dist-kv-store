package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/ski-akash/dist-kv-store/internal/raft"
	"github.com/ski-akash/dist-kv-store/internal/server"
	"github.com/ski-akash/dist-kv-store/internal/storage"
	pb "github.com/ski-akash/dist-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parse command line flags for cluster setup
	nodeID := flag.String("id", "node1", "Unique ID for this node")
	port := flag.String("port", "50051", "Port to listen on")
	peersFlag := flag.String("peers", "", "Comma-separated list of peer addresses (e.g., localhost:50052,localhost:50053)")
	flag.Parse()

	peers := []string{}
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	// 1. Initialize Storage (Use unique WAL file per node)
	wal, err := storage.NewWAL(fmt.Sprintf("%s.wal", *nodeID))
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	store := storage.NewKVStore()
	store.RecoverFromWAL(wal)

	// 2. Initialize Raft Node
	raftNode := raft.NewNode(*nodeID, peers)

	// 3. Set up the gRPC Server
	grpcServer := grpc.NewServer()

	// Register the KV Store API
	kvServer := server.NewKVServer(store, wal)
	pb.RegisterKVStoreServer(grpcServer, kvServer)

	// Register the Raft Internal API
	pb.RegisterRaftNodeServer(grpcServer, raftNode)

	// 4. Connect to Peers (Background routine)
	go func() {
		for _, peerAddr := range peers {
			conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				client := pb.NewRaftNodeClient(conn)
				raftNode.AddClient(peerAddr, client) // Helper method to add to clients map
			}
		}
	}()

	// 5. Start Listening
	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", *port, err)
	}

	log.Printf("🚀 Node %s starting on port %s...", *nodeID, *port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
