package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	pb "distr-scheduling/api/proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fs := flag.NewFlagSet("slctl", flag.ExitOnError)
	addr := fs.String("controller", envOr("SLURMLITE_CONTROLLER", "127.0.0.1:50051"), "controller gRPC address")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), `Usage: slctl [global flags] <command> [args]

Commands:
  submit   Submit a job to the queue
  queue    List pending, running, and finished jobs
  cancel   Cancel a pending or running job
  nodes    List cluster nodes

Global flags:
`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	args := fs.Args()
	if len(args) == 0 {
		fs.Usage()
		os.Exit(2)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "slctl: dial %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewControllerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "submit":
		err = runSubmit(ctx, client, rest)
	case "queue":
		err = runQueue(ctx, client, rest)
	case "cancel":
		err = runCancel(ctx, client, rest)
	case "nodes":
		err = runNodes(ctx, client, rest)
	default:
		fmt.Fprintf(os.Stderr, "slctl: unknown command %q\n", cmd)
		fs.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "slctl %s: %v\n", cmd, err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
