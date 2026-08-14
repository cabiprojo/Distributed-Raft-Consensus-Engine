package raft

import (
	"math/rand"
	"sync"
	"time"

	pb "raft-kv/proto"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// node holds all persistent and volatile state for a single Raft node
type Node struct {
	pb.UnimplementedRaftServiceServer

	mu sync.Mutex

	id    string
	state NodeState

	peers         []string
	lastHeartbeat time.Time

	// Persistent state (paper Figure 2)
	currentTerm int64
	votedFor    string
	log         []*pb.LogEntry
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) runElectionTimer() {
	timeout := randomElectionTimeout()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		state := n.state
		elapsed := time.Since(n.lastHeartbeat)
		n.mu.Unlock()

		if state == Leader {
			continue
		}

		if elapsed >= timeout {
			n.startElection()
			timeout = randomElectionTimeout()
		}
	}
}

func (n *Node) startElection() {
	// TODO: implement
}
