# raft-kv

A from-scratch implementation of the Raft consensus algorithm in Go, built
into a replicated key-value store. Built as a learning project: every
piece of the core logic (leader election, log replication, commit safety,
crash recovery) is implemented and understood by hand, not generated.

## Status

Working end-to-end. A 3-5 node cluster elects and maintains a single
leader, replicates `SET`/`GET`/`DELETE` operations to a majority before
committing them, persists state to disk so nodes recover correctly across
a crash/restart, and survives a leader failure mid-write with zero loss of
any acknowledged write.

## What's implemented

- **Leader election**: randomized election timeouts, term-based logical
  clock, the full `RequestVote` vote-granting rules (term check,
  one-vote-per-term, the log-up-to-date safety restriction).
- **Log replication**: real `AppendEntries` replication using
  `nextIndex`/`matchIndex` per follower, the `prevLogIndex`/`prevLogTerm`
  consistency check with conflict-entry truncation, and `commitIndex`
  advancement that respects the Figure 8 restriction (a leader may only
  directly commit entries from its own current term).
- **Persistence**: `currentTerm`, `votedFor`, and the log are written to
  disk before replying to any RPC that changes them, so a crashed and
  restarted node never double-votes or silently loses committed data.
- **Client interface**: a `ClientService` gRPC (`Put`/`Get`/`Delete`) plus
  a small CLI client (`cmd/client`) for driving reads/writes against any
  node.
- **Verified failure recovery**: a 3-node cluster was run with client
  writes going through the leader, the leader process was killed
  mid-sequence, the cluster re-elected a new leader, writes resumed, every
  previously-acknowledged write was confirmed present and consistent on
  the surviving nodes, and the killed node was restarted and confirmed to
  rejoin and catch up correctly (via both its own persisted state and log
  replication from the new leader).

## What's deliberately out of scope

- **Snapshotting / log compaction**: the log grows unboundedly. Fine for
  a demo/learning project, a production system would need this to bound
  disk usage and rejoin time for long-lagging followers.
- **Cluster membership changes**: the peer set is fixed at startup via
  `-peers`, Raft's joint-consensus membership-change protocol isn't
  implemented.
- **Broader chaos testing**: dropped/delayed RPCs and network partitions
  beyond a hard process kill weren't scripted. The one failure scenario
  above (kill leader mid-write) was run rigorously, a full partition/delay
  test matrix was scoped out for time.
- **Linearizable reads**: `Get` reads a node's local applied state
  directly. On the leader this is normally fresh, but it isn't proven
  linearizable (no read-index/lease mechanism), a stale leader that
  hasn't yet learned it's been deposed could theoretically serve a stale
  read for a brief window.
- **TLS between nodes**: internal RPCs use insecure gRPC credentials,
  fine for localhost, not for a real deployment.

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
