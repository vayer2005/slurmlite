package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"distr-scheduling/internal/cluster"
	"distr-scheduling/internal/controller"
	"distr-scheduling/internal/job"
)

func main() {
	listen := flag.String("listen", ":50051", "gRPC listen address")
	timeout := flag.Duration("heartbeat-timeout", cluster.DefaultHeartbeatTimeout, "mark a node offline after this much silence")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry := cluster.Make()
	jobs := job.Make()
	mon := cluster.MakeMonitor(registry, *timeout, 0, jobs)
	go mon.Run(ctx)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcSrv := controller.NewGRPCServer(controller.Make(registry, mon))
	go func() {
		<-ctx.Done()
		grpcSrv.GracefulStop()
	}()

	log.Printf("controller listening on %s (heartbeat timeout %s)", lis.Addr(), *timeout)
	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
