package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	id := flag.String("id", "", "unique id for this node (e.g. node1)")
	port := flag.String("port", "", "port this node listens on (e.g. 50051)")
	peers := flag.String("peers", "", "comma-separated host:port list of peer nodes")
	flag.Parse()

	if *id == "" || *port == "" {
		log.Fatal("must provide -id and -port")
	}

	fmt.Printf("starting node %s on port %s, peers=%s\n", *id, *port, *peers)

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// TODO(you): once raft.proto is filled in and generated, register the
	// RaftService server here, e.g.:
	//   pb.RegisterRaftServiceServer(grpcServer, raftNode)

	log.Printf("node %s listening on :%s", *id, *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
