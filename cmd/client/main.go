package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "raft-kv/proto"
)

func main() {
	addr := flag.String("addr", "", "node address to talk to, e.g. localhost:50051")
	op := flag.String("op", "", "operation: put | get | delete")
	key := flag.String("key", "", "key")
	value := flag.String("value", "", "value (for put)")
	flag.Parse()

	if *addr == "" || *op == "" || *key == "" {
		log.Fatal("must provide -addr, -op, and -key")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewClientServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	switch *op {
	case "put":
		reply, err := client.Put(ctx, &pb.PutRequest{Key: *key, Value: *value})
		if err != nil {
			log.Fatalf("rpc error: %v", err)
		}
		fmt.Printf("success=%v error=%q\n", reply.Success, reply.Error)
	case "get":
		reply, err := client.Get(ctx, &pb.GetRequest{Key: *key})
		if err != nil {
			log.Fatalf("rpc error: %v", err)
		}
		fmt.Printf("found=%v value=%q\n", reply.Found, reply.Value)
	case "delete":
		reply, err := client.Delete(ctx, &pb.DeleteRequest{Key: *key})
		if err != nil {
			log.Fatalf("rpc error: %v", err)
		}
		fmt.Printf("success=%v error=%q\n", reply.Success, reply.Error)
	default:
		log.Fatalf("unknown op %q", *op)
	}
}
