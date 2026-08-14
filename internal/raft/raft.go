package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "raft-kv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NodeState represents the state of a Raft node (Follower, Candidate, or Leader)
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// node holds all persistent and volatile state for a single Raft node
type Node struct {
	pb.UnimplementedRaftServiceServer // embed the unimplemented server for forward compatibility

	mu sync.Mutex

	id    string
	state NodeState

	peers         []string
	lastHeartbeat time.Time

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
		// mutex lock to safely read state and lastHeartbeat
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
	// mutex lock to safely update state and term
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	n.state = Candidate
	n.lastHeartbeat = time.Now()

	term := n.currentTerm
	candidateID := n.id
	peers := n.peers

	lastLogIndex := int64(len(n.log) - 1)
	var lastLogTerm int64
	if len(n.log) > 0 {
		lastLogTerm = n.log[len(n.log)-1].Term
	}
	n.mu.Unlock()

	log.Printf("[%s] starting election for term %d", n.id, term)

	total := len(peers) + 1
	votesNeeded := total/2 + 1
	voteCh := make(chan bool, len(peers))

	// send RequestVote RPCs to all peers
	for _, peer := range peers {
		go func(peer string) {
			conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				voteCh <- false
				return
			}
			defer conn.Close()

			client := pb.NewRaftServiceClient(conn)
			reply, err := client.RequestVote(context.Background(), &pb.RequestVoteArgs{
				Term:         term,
				CandidateId:  candidateID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				voteCh <- false
				return
			}
			voteCh <- reply.VoteGranted
		}(peer)
	}

	votes := 1 // vote for self
	for range peers {
		if <-voteCh {
			votes++
		}
		if votes >= votesNeeded {
			break
		}
	}

	n.mu.Lock()
	won := votes >= votesNeeded && n.state == Candidate && n.currentTerm == term
	if won {
		n.state = Leader
		log.Printf("[%s] won election, becomes leader for term %d", n.id, term)
	}
	n.mu.Unlock()
}

// RequestVote implements the RaftServiceServer interface. It runs on the
// receiving/voting node whenever a candidate asks for its vote.
func (n *Node) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Stale term is an immediate deny.
	if args.Term < n.currentTerm {
		return &pb.RequestVoteReply{Term: n.currentTerm, VoteGranted: false}, nil
	}

	// A higher term means we're behind: adopt it and step down, regardless
	// of whether we end up granting this particular vote.
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.state = Follower
	}

	// One vote per term.
	canVote := n.votedFor == "" || n.votedFor == args.CandidateId

	// Candidate's log must be at least as up-to-date as ours.
	lastLogIndex := int64(len(n.log) - 1)
	var lastLogTerm int64
	if len(n.log) > 0 {
		lastLogTerm = n.log[len(n.log)-1].Term
	}
	logUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if canVote && logUpToDate {
		n.votedFor = args.CandidateId
		n.lastHeartbeat = time.Now()
		return &pb.RequestVoteReply{Term: n.currentTerm, VoteGranted: true}, nil
	}

	return &pb.RequestVoteReply{Term: n.currentTerm, VoteGranted: false}, nil
}
