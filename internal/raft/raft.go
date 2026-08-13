package raft

import (
	"sync"
	"time"

	pb "raft-kv/proto"
)

// NodeState represents which role a Raft node currently holds.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// Node holds all persistent and volatile state for a single Raft node.
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
