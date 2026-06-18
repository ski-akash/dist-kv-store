package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/ski-akash/dist-kv-store/proto"
)

// Define the three possible states for a Raft node
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

// Node represents a single Raft consensus server
type Node struct {
	mu sync.Mutex

	// Basic state
	id    string
	peers []string // Addresses of all other nodes in the cluster
	state NodeState

	// Persistent state on all servers (updated before responding to RPCs)
	currentTerm int32
	votedFor    string // The candidateId that received vote in current term (or "" if none)
	log         []*pb.RaftLogEntry

	// Volatile state on all servers
	commitIndex int32
	lastApplied int32

	// Election timing
	electionResetEvent time.Time
}

// NewNode initializes a Raft node
func NewNode(id string, peers []string) *Node {
	rn := &Node{
		id:          id,
		peers:       peers,
		state:       StateFollower,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*pb.RaftLogEntry, 0),
	}

	// Start the background election timer
	go rn.runElectionTimer()

	return rn
}

// runElectionTimer continuously runs in the background.
// If the timer expires without a heartbeat from a leader, this node starts an election.
func (rn *Node) runElectionTimer() {
	// Raft requires randomized timeouts to prevent split votes
	timeoutDuration := rn.randomTimeout()
	rn.mu.Lock()
	rn.electionResetEvent = time.Now()
	rn.mu.Unlock()

	for {
		time.Sleep(10 * time.Millisecond) // Check every 10ms

		rn.mu.Lock()
		elapsed := time.Since(rn.electionResetEvent)
		state := rn.state
		rn.mu.Unlock()

		// If we are not the leader, and we haven't heard a heartbeat recently...
		if state != StateLeader && elapsed >= timeoutDuration {
			// Start a new election!
			rn.startElection()

			// Reset the timer for the next potential election
			timeoutDuration = rn.randomTimeout()
			rn.mu.Lock()
			rn.electionResetEvent = time.Now()
			rn.mu.Unlock()
		}
	}
}

// randomTimeout returns a duration between 150ms and 300ms
func (rn *Node) randomTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// startElection triggers the transition from Follower to Candidate
func (rn *Node) startElection() {
	rn.mu.Lock()
	rn.state = StateCandidate
	rn.currentTerm++
	rn.votedFor = rn.id // Vote for self
	savedTerm := rn.currentTerm
	rn.mu.Unlock()

	log.Printf("Node %s becomes Candidate, starting election for term %d", rn.id, savedTerm)

	
}
