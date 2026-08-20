# Distributed Raft Consensus Engine

A from-scratch implementation of the Raft consensus algorithm in Go,
built into a working replicated key-value store. It covers leader
election, log replication, crash recovery, and the safety rules that keep
data consistent across nodes even when one of them fails.

## Status

It works end to end. Spin up 3 to 5 nodes and they'll elect one leader and
keep it, replicate SET/GET/DELETE writes to a majority of nodes before
counting them as done, save state to disk so a node comes back correctly
after a crash, and survive losing the leader mid-write without losing any
write that was already confirmed.

## What's implemented

- **Leader election**: nodes use randomized timeouts so they don't all try
  to become leader at once, plus a term counter that acts like a logical
  clock. The vote-granting rules cover the important edge case too: a
  candidate only gets votes if its log is at least as up to date as the
  voter's, which is what guarantees a new leader never "forgets" data
  that already made it to a majority of nodes.
- **Log replication**: the leader tracks what each follower actually has
  (`nextIndex`/`matchIndex`) and sends only what they're missing. There's
  a consistency check that catches and fixes any follower whose log
  diverged from the leader's. Committing an entry follows the trickiest
  safety rule in Raft (Figure 8 in the paper): a leader can only directly
  commit entries from its own current term, older entries ride along once
  a newer one commits.
- **Persistence**: a node writes its term, its vote, and its log to disk
  before it ever replies to a request that changed them. That ordering
  matters, it's what stops a node from voting twice in the same term if
  it crashes and comes back up with amnesia.
- **Client interface**: a small gRPC service (`Put`/`Get`/`Delete`) plus a
  CLI (`cmd/client`) so you can actually read and write to the cluster
  instead of just watching logs.
- **Verified failure recovery**: I ran a real 3-node cluster, wrote some
  keys through the leader, killed the leader process mid-run, and watched
  the cluster elect a new one and keep going. Every write that had
  already been confirmed was still there afterward, on every surviving
  node. I also restarted the node I killed and confirmed it rejoined and
  caught back up correctly.

## What I left out on purpose

- **Snapshotting / log compaction**: the log just grows forever right now.
  That's fine for a project like this, but a real production system would
  need a way to compact it so disk usage and rejoin time don't blow up.
- **Changing cluster membership on the fly**: the list of peers is fixed
  when a node starts. Raft has a protocol for safely adding or removing
  nodes while the cluster is running, I didn't build that.
- **Deeper chaos testing**: I tested the main failure case you'd actually
  care about (leader dies mid-write) thoroughly, but I didn't build
  tooling to simulate things like dropped or delayed network messages, or
  a real network partition. That's the natural next thing to add.
- **Linearizable reads**: reads are served from whatever a node has
  locally applied. That's usually fine on the leader, but it's not
  formally guaranteed to always be fresh, a leader that just got replaced
  but doesn't know it yet could theoretically hand back a stale
  read for a brief window.
- **TLS between nodes**: nodes talk to each other over plain, unencrypted
  gRPC. That's fine on localhost for a demo, not something you'd want in
  a real deployment.

## Setup (run these locally, e.g. in VS Code with Claude Code)

1. Install Go 1.22+: https://go.dev/doc/install
2. Install protoc and the Go plugins:
   ```
   brew install protobuf   # or apt/other package manager
   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
   ```
   Make sure `$GOPATH/bin` (usually `~/go/bin`) is on your `$PATH`.

3. Fetch dependencies:
   ```
   go mod tidy
   ```

4. Generate the gRPC code (only needed after editing `proto/raft.proto`):
   ```
   protoc --go_out=. --go_opt=paths=source_relative \
          --go-grpc_out=. --go-grpc_opt=paths=source_relative \
          proto/raft.proto
   ```
   This generates `proto/raft.pb.go` and `proto/raft_grpc.pb.go`.

5. Run a 3-node cluster (each in its own terminal):
   ```
   go run ./cmd/node -id node1 -port 50051 -peers localhost:50052,localhost:50053 -datadir ./data
   go run ./cmd/node -id node2 -port 50052 -peers localhost:50051,localhost:50053 -datadir ./data
   go run ./cmd/node -id node3 -port 50053 -peers localhost:50051,localhost:50052 -datadir ./data
   ```
   One will log `won election, becomes leader for term N`. Persisted state
   for each node is written to `./data/<id>-state.json`.

6. Read/write against the leader (client fails with an error if pointed at
   a non-leader node, it does not auto-redirect):
   ```
   go run ./cmd/client -addr localhost:50051 -op put -key foo -value bar
   go run ./cmd/client -addr localhost:50051 -op get -key foo
   go run ./cmd/client -addr localhost:50051 -op delete -key foo
   ```

7. To see failure recovery yourself: kill the leader's process (e.g.
   `Stop-Process` / `kill -9`), watch the remaining nodes' logs for a new
   `won election` line, then read/write against the new leader, every
   previously-acknowledged write is still there.

## Project layout

- `proto/raft.proto`: RPC contract (`RaftService` for node-to-node
  consensus, `ClientService` for external reads/writes)
- `internal/raft/raft.go`: the full Raft state machine, election,
  replication, persistence, commit rules, KV state machine
- `cmd/node/`: process entry point for a single Raft node
- `cmd/client/`: CLI for issuing `Put`/`Get`/`Delete` against a node
