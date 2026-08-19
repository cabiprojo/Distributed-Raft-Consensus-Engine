package main

import (
	"flag"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"

	"raft-kv/internal/raft"
	pb "raft-kv/proto"
)

func main() {
	id := flag.String("id", "", "unique id for this node (e.g. node1)")
	port := flag.String("port", "", "port this node listens on (e.g. 50051)")
	peers := flag.String("peers", "", "comma-separated host:port list of peer nodes")
	flag.Parse()

	if *id == "" || *port == "" {
		log.Fatal("must provide -id and -port")
	}

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	raftNode := raft.NewNode(*id, peerList)
	pb.RegisterRaftServiceServer(grpcServer, raftNode)
	pb.RegisterClientServiceServer(grpcServer, raftNode)
	raftNode.Start()

	log.Printf("node %s listening on :%s", *id, *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
