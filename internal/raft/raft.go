package raft

import (
	"context"
	"log"
	"math/rand"
	"strings"
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

	// volatile state on all servers
	commitIndex int64
	lastApplied int64

	// volatile state on leaders only, reinitialized after each election
	nextIndex  map[string]int64
	matchIndex map[string]int64

	// the actual replicated state machine - applied to as entries commit
	kv map[string]string
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
		kv:            make(map[string]string),
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
	n.mu.Lock()
	n.nextIndex = make(map[string]int64)
	n.matchIndex = make(map[string]int64)
	for _, peer := range n.peers {
		n.nextIndex[peer] = int64(len(n.log)) + 1
		n.matchIndex[peer] = 0
	}
	n.mu.Unlock()

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
			go n.replicateToPeer(term, id, peer)
		}
	}
}

// replicateToPeer sends one AppendEntries RPC to a single peer, containing
// whatever entries that peer is missing according to nextIndex, then
// updates nextIndex/matchIndex based on the result.
func (n *Node) replicateToPeer(term int64, id string, peer string) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	var prevLogTerm int64
	if prevLogIndex > 0 && prevLogIndex <= int64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	var entries []*pb.LogEntry
	if nextIdx <= int64(len(n.log)) {
		entries = n.log[nextIdx-1:]
	}
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	reply, err := client.AppendEntries(context.Background(), &pb.AppendEntriesArgs{
		Term:         term,
		LeaderId:     id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.votedFor = ""
		n.state = Follower
		log.Printf("[%s] stepping down: saw higher term %d in AppendEntries reply", n.id, reply.Term)
		return
	}

	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		n.matchIndex[peer] = prevLogIndex + int64(len(entries))
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitIndexLocked(term)
	} else if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
}

// advanceCommitIndexLocked checks whether commitIndex can move forward.
// Per the Figure 8 safety rule, an entry can only be committed by direct
// majority count if it was created during the leader's own current term;
// older entries become committed indirectly once a current-term entry
// past them commits. Caller must hold n.mu.
func (n *Node) advanceCommitIndexLocked(term int64) {
	for N := int64(len(n.log)); N > n.commitIndex; N-- {
		if n.log[N-1].Term != term {
			continue
		}
		count := 1 // the leader itself has it
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				count++
			}
		}
		total := len(n.peers) + 1
		if count >= total/2+1 {
			n.commitIndex = N
			n.applyCommittedLocked()
			log.Printf("[%s] commitIndex advanced to %d", n.id, N)
			return
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

	// higher term means this node is behind. adopt it and step down to follower
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}

	// a candidate steps down on an EQUAL term too, not just a higher one
	// this is what handles a candidate discovering a legitimate leader that
	// already won the same term it's currently campaigning for
	n.state = Follower
	n.lastHeartbeat = time.Now()

	// consistency check: PrevLogIndex 0 means "nothing should come before this" and always matches
	// otherwise we must already have an entry at PrevLogIndex whose term matches PrevLogTerm
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > int64(len(n.log)) || n.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			return &pb.AppendEntriesReply{Term: n.currentTerm, Success: false}, nil
		}
	}

	// walk the incoming entries: the first one that's missing or conflicts
	// with what we have is where we truncate (discarding it and everything
	// after those can only be leftovers from an old, uncommitted leader)
	// and append the leader's version from that point on
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + int64(i) + 1
		if idx <= int64(len(n.log)) && n.log[idx-1].Term == entry.Term {
			continue
		}
		n.log = append(n.log[:idx-1], args.Entries[i:]...)
		break
	}

	// update commitIndex and apply any newly committed entries to the kv state machine
	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + int64(len(args.Entries))
		n.commitIndex = min(args.LeaderCommit, lastNewIndex)
		n.applyCommittedLocked()
	}

	return &pb.AppendEntriesReply{Term: n.currentTerm, Success: true}, nil
}

// applyCommittedLocked applies log entries between lastApplied and commitIndex to the kv state machine
// caller must hold n.mu
func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		applyCommand(n.kv, entry.Command)
	}
}

// applyCommand parses and applies a single encoded command against kv
// Encoding is deliberately simple: "SET key value" or "DELETE key"
func applyCommand(kv map[string]string, command string) {
	parts := strings.SplitN(command, " ", 3)
	if len(parts) == 0 {
		return
	}
	switch parts[0] {
	case "SET":
		if len(parts) == 3 {
			kv[parts[1]] = parts[2]
		}
	case "DELETE":
		if len(parts) >= 2 {
			delete(kv, parts[1])
		}
	}
}
