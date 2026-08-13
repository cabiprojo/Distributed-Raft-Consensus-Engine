# raft-kv

A from-scratch implementation of the Raft consensus algorithm in Go, built
toward a distributed key-value store. Built as a learning project — every
piece of the core logic (election, log replication, commit rules) is
implemented and understood by hand, not generated.

## Status

Scaffolding stage. Core Raft logic not yet implemented.

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

4. Once `proto/raft.proto` fields are filled in, generate the gRPC code:
   ```
   protoc --go_out=. --go_opt=paths=source_relative \
          --go-grpc_out=. --go-grpc_opt=paths=source_relative \
          proto/raft.proto
   ```
   This generates `proto/raft.pb.go` and `proto/raft_grpc.pb.go`.

5. Run a node:
   ```
   go run ./cmd/node -id node1 -port 50051 -peers localhost:50052,localhost:50053
   ```

## Project layout

- `proto/raft.proto` — RPC contract (RequestVote, AppendEntries)
- `internal/raft/` — core Raft state machine (election, replication, safety)
- `cmd/node/` — process entry point for a single Raft node

## Scope

TODO: fill in as the project progresses — what's implemented, what's
deliberately left out (e.g. snapshotting/log compaction), and why.
