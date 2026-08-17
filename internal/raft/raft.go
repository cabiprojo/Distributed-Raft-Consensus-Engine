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

// NewNode constructs a Node in the Follower state, ready to be started
// peers is the list of other nodes' "host:port" addresses
func NewNode(id string, peers []string) *Node {
	return &Node{
		id:            id,
		state:         Follower,
		peers:         peers,
		lastHeartbeat: time.Now(),
	}
}

// Start begins the node's background election-timeout goroutine
// call this once, after registering the Node with a gRPC server
func (n *Node) Start() {
	go n.runElectionTimer()
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

	lastLogIndex := int64(len(n.log))
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

	if won {
		go n.runLeaderHeartbeats(term)
	}
}

// runLeaderHeartbeats periodically sends empty AppendEntries to every peer
// to maintain this node's leadership for the given term. It exits as soon
// as this node is no longer the leader of that term.
func (n *Node) runLeaderHeartbeats(term int64) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		peers := n.peers
		id := n.id
		n.mu.Unlock()

		for _, peer := range peers {
			go func(peer string) {
				conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return
				}
				defer conn.Close()

				client := pb.NewRaftServiceClient(conn)
				reply, err := client.AppendEntries(context.Background(), &pb.AppendEntriesArgs{
					Term:     term,
					LeaderId: id,
				})
				if err != nil {
					return
				}

				if reply.Term > term {
					n.mu.Lock()
					if reply.Term > n.currentTerm {
						n.currentTerm = reply.Term
						n.votedFor = ""
						n.state = Follower
						log.Printf("[%s] stepping down: saw higher term %d in AppendEntries reply", n.id, reply.Term)
					}
					n.mu.Unlock()
				}
			}(peer)
		}
	}
}

// RequestVote implements the RaftServiceServer interface
// it runs on the receiving/voting node whenever a candidate asks for its vote
func (n *Node) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// stale term is instant deny
	if args.Term < n.currentTerm {
		return &pb.RequestVoteReply{Term: n.currentTerm, VoteGranted: false}, nil
	}

	// a higher term means we're behind: adopt it and step down
	// regardless of whether we end up granting this particular vote.
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.state = Follower
	}

	// one vote per term
	canVote := n.votedFor == "" || n.votedFor == args.CandidateId

	// candidate's log must be at least as up-to-date as ours
	lastLogIndex := int64(len(n.log))
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

// AppendEntries implements the RaftServiceServer interface. For now this
// only handles the heartbeat case (empty entries) - log replication comes
// in a later commit.
func (n *Node) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// stale leader: reject outright
	if args.Term < n.currentTerm {
		return &pb.AppendEntriesReply{Term: n.currentTerm, Success: false}, nil
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}

	// a candidate steps down on an EQUAL term too, not just a higher one -
	// this is what handles a candidate discovering a legitimate leader that
	// already won the same term it's currently campaigning for
	n.state = Follower
	n.lastHeartbeat = time.Now()

	return &pb.AppendEntriesReply{Term: n.currentTerm, Success: true}, nil
}
