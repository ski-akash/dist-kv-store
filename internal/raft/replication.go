package raft

import (
	"context"
	"time"

	pb "github.com/ski-akash/dist-kv-store/proto"
)

// AppendEntries is called by the Leader to replicate logs and send heartbeats.
func (rn *Node) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 1. Reject the request if the sender's term is older than ours
	if req.Term < rn.currentTerm {
		return &pb.AppendResponse{Term: rn.currentTerm, Success: false}, nil
	}

	// 2. If the sender's term is newer (or we were a Candidate), we recognize them as Leader
	if req.Term > rn.currentTerm || rn.state != StateFollower {
		rn.becomeFollower(req.Term)
	}

	// 3. THE MAGIC LINE: Reset our election timer because we heard from the active Leader!
	rn.electionResetEvent = time.Now()

	// (Note: In a full Raft implementation, we would also append log entries to our disk here.
	// For this phase, we are just focusing on the heartbeat mechanism.)

	return &pb.AppendResponse{Term: rn.currentTerm, Success: true}, nil
}

// startHeartbeats runs in a loop, broadcasting heartbeats to all peers
func (rn *Node) startHeartbeats() {
	for {
		rn.mu.Lock()
		if rn.state != StateLeader {
			rn.mu.Unlock()
			return // Stop sending heartbeats if we are no longer the leader
		}
		savedTerm := rn.currentTerm
		rn.mu.Unlock()

		// Send heartbeats to all peers concurrently
		for _, peer := range rn.peers {
			go func(peerID string) {
				rn.mu.Lock()
				client, ok := rn.clients[peerID]
				rn.mu.Unlock()

				if !ok {
					return
				}

				req := &pb.AppendRequest{
					Term:     savedTerm,
					LeaderId: rn.id,
				}

				// Send the RPC
				resp, err := client.AppendEntries(context.Background(), req)
				if err != nil {
					return // Peer might be offline, ignore for now
				}

				// If the peer replied with a higher term, we have been deposed! Step down.
				rn.mu.Lock()
				defer rn.mu.Unlock()
				if resp.Term > rn.currentTerm {
					rn.becomeFollower(resp.Term)
				}
			}(peer)
		}

		// Sleep for 50ms. This MUST be much shorter than our 150-300ms election timeout!
		time.Sleep(50 * time.Millisecond)
	}
}
