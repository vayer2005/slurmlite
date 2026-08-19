package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	pb "distr-scheduling/api/proto/v1"
	"distr-scheduling/internal/node"
)

func main() {
	addr := flag.String("controller", "127.0.0.1:50051", "controller gRPC address")
	id := flag.String("id", "", "node id (defaults to hostname)")
	cpus := flag.Int("cpus", 1, "total CPUs on this node")
	interval := flag.Duration("heartbeat-interval", node.DefaultHeartbeatInterval, "how often to send Heartbeat RPCs")
	flag.Parse()

	hostname, _ := os.Hostname()
	if *id == "" {
		*id = hostname
	}
	if *id == "" {
		log.Fatal("node id is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := node.Dial(*addr)
	if err != nil {
		log.Fatalf("dial controller: %v", err)
	}
	defer conn.Close()

	agent := node.Make(pb.NewControllerServiceClient(conn), node.Config{
		ID:        *id,
		Hostname:  hostname,
		TotalCPUs: *cpus,
		Interval:  *interval,
	})

	log.Printf("node %s connected to %s (heartbeat every %s)", *id, *addr, *interval)
	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
