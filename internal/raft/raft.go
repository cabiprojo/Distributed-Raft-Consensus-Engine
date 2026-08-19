package raft

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"os"
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
	pb.UnimplementedClientServiceServer

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

	// where persistent state (currentTerm, votedFor, log) is durably saved
	persistPath string
}

// persistedState is the on-disk representation of a Node's persistent state
// everything that must survive a crash and restart
type persistedState struct {
	CurrentTerm int64
	VotedFor    string
	Log         []*pb.LogEntry
}

// persistLocked durably writes currentTerm/votedFor/log to disk
// caller must hold n.mu
// must be called BEFORE replying to any RPC that changed these fields
// see the persist-before-reply safety rule
func (n *Node) persistLocked() {
	if n.persistPath == "" {
		return
	}
	data, err := json.MarshalIndent(persistedState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}, "", "  ")
	if err != nil {
		log.Printf("[%s] failed to marshal persistent state: %v", n.id, err)
		return
	}
	if err := os.WriteFile(n.persistPath, data, 0600); err != nil {
		log.Printf("[%s] failed to persist state to %s: %v", n.id, n.persistPath, err)
	}
}

// loadPersisted reads previously-saved state from disk, if any exists
// returns the zero state (fresh node) if the file doesn't exist yet
func loadPersisted(path string) (persistedState, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return persistedState{}, nil
	}
	if err != nil {
		return persistedState{}, err
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return persistedState{}, err
	}
	return ps, nil
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// NewNode constructs a Node in the Follower state, ready to be started
// peers is the list of other nodes' "host:port" addresses
// persistPath is where currentTerm/votedFor/log are durably saved
// pass "" to run in-memory only
// if persistPath already has saved state, it's loaded here
func NewNode(id string, peers []string, persistPath string) *Node {
	ps, err := loadPersisted(persistPath)
	if err != nil {
		log.Printf("[%s] failed to load persisted state, starting fresh: %v", id, err)
	}
	return &Node{
		id:            id,
		state:         Follower,
		peers:         peers,
		lastHeartbeat: time.Now(),
		kv:            make(map[string]string),
		persistPath:   persistPath,
		currentTerm:   ps.CurrentTerm,
		votedFor:      ps.VotedFor,
		log:           ps.Log,
	}
}

// Start begins the node's background election-timeout goroutine
// call this once, after registering the Node with a gRPC server
func (n *Node) Start() {
	go n.runElectionTimer()
}

// ErrNotLeader is returned by Propose when called on a non-leader node
var ErrNotLeader = errors.New("not the leader")

// Propose appends command to the leader's log
// blocks until the entry is committed and applied, or the timeout elapses
func (n *Node) Propose(command string) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	index := int64(len(n.log)) + 1
	n.log = append(n.log, &pb.LogEntry{
		Term:    n.currentTerm,
		Index:   index,
		Command: command,
	})
	n.persistLocked()
	n.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		applied := n.lastApplied >= index
		stillLeader := n.state == Leader
		n.mu.Unlock()

		if applied {
			return nil
		}
		if !stillLeader {
			return errors.New("lost leadership before entry committed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("timed out waiting for entry to commit")
}

// getLocal reads the current value for key from this node's local state machine
// only reflects committed, applied writes on this specific node
func (n *Node) getLocal(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.kv[key]
	return v, ok
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
	n.persistLocked()

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
// to maintain this node's leadership for the given term
// it exits as soon as this node is no longer the leader of that term
func (n *Node) runLeaderHeartbeats(term int64) {
	// initialize nextIndex and matchIndex for all peers
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

// replicateToPeer sends one AppendEntries RPC to a single peer
// containing whatever entries that peer is missing according to nextIndex
// updates nextIndex/matchIndex based on the result
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

// advanceCommitIndexLocked checks whether commitIndex can move forward
// Per the Figure 8 safety rule, an entry can only be committed by direct
// majority count if it was created during the leader's own current term
// older entries become committed indirectly once a current-term entry
// past them commits. Caller must hold n.mu
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
	// regardless of whether we end up granting this particular vote
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.state = Follower
		n.persistLocked()
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
		n.persistLocked()
		return &pb.RequestVoteReply{Term: n.currentTerm, VoteGranted: true}, nil
	}

	return &pb.RequestVoteReply{Term: n.currentTerm, VoteGranted: false}, nil
}

// AppendEntries implements the RaftServiceServer interface
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
		n.persistLocked()
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
		n.persistLocked()
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

// Put implements ClientServiceServer. Only succeeds on the current leader
func (n *Node) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutReply, error) {
	if err := n.Propose("SET " + req.Key + " " + req.Value); err != nil {
		return &pb.PutReply{Success: false, Error: err.Error()}, nil
	}
	return &pb.PutReply{Success: true}, nil
}

// Delete implements ClientServiceServer
func (n *Node) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.PutReply, error) {
	if err := n.Propose("DELETE " + req.Key); err != nil {
		return &pb.PutReply{Success: false, Error: err.Error()}, nil
	}
	return &pb.PutReply{Success: true}, nil
}

// Get implements ClientServiceServer
// reads local applied state directly only guaranteed fresh when called on the leader
func (n *Node) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	value, found := n.getLocal(req.Key)
	return &pb.GetReply{Found: found, Value: value}, nil
}
