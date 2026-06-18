package raft

import (
	"context"
	"log"

	pb "github.com/ski-akash/dist-kv-store/proto"
)

// RequestVote is the RPC handler called by candidates to gather votes
func (rn *Node) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 1. Reply false if candidate's term is older than ours
	if req.Term < rn.currentTerm {
		log.Printf("Node %s rejecting vote for %s (Candidate Term: %d < Current Term: %d)",
			rn.id, req.CandidateId, req.Term, rn.currentTerm)
		return &pb.VoteResponse{Term: rn.currentTerm, VoteGranted: false}, nil
	}

	// 2. If the request has a higher term, we must step down and become a follower
	if req.Term > rn.currentTerm {
		rn.becomeFollower(req.Term)
	}

	// 3. Check if we haven't voted yet, or already voted for this specific candidate
	if rn.votedFor == "" || rn.votedFor == req.CandidateId {
		// Grant the vote!
		rn.votedFor = req.CandidateId
		log.Printf("Node %s granting vote to %s for term %d", rn.id, req.CandidateId, req.Term)
		return &pb.VoteResponse{Term: rn.currentTerm, VoteGranted: true}, nil
	}

	// Otherwise, we already voted for someone else
	return &pb.VoteResponse{Term: rn.currentTerm, VoteGranted: false}, nil
}

// becomeFollower safely transitions a node back to a follower state
func (rn *Node) becomeFollower(newTerm int32) {
	rn.state = StateFollower
	rn.currentTerm = newTerm
	rn.votedFor = ""
	log.Printf("Node %s stepping down to Follower for term %d", rn.id, newTerm)
}

// becomeLeader safely transitions a candidate to a leader
func (rn *Node) becomeLeader() {
	rn.state = StateLeader
	log.Printf("🏆 Node %s HAS WON THE ELECTION AND IS NOW THE LEADER! 🏆", rn.id)

}
