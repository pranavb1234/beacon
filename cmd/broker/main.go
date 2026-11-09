package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pranavb1234/beacon/internal/broker"
	"github.com/pranavb1234/beacon/internal/cluster"
	"github.com/pranavb1234/beacon/internal/config"
)

func main() {
	cfgPath := flag.String("config", "", "path to YAML config file (optional; env vars override)")
	healthcheck := flag.Bool("healthcheck", false, "run healthcheck against broker address and exit")
	healthAddr := flag.String("addr", "", "address for healthcheck probe (defaults to 127.0.0.1:9001)")
	topicOwner := flag.String("topic-owner", "", "admin flag: print the owner broker of the specified topic and exit")
	flag.Parse()

	// 1. Healthcheck probe
	if *healthcheck {
		target := *healthAddr
		if target == "" {
			target = os.Getenv("BEACON_LISTEN_ADDR")
			if target == "" {
				target = "127.0.0.1:9001"
			}
		}
		if strings.HasPrefix(target, ":") {
			target = "127.0.0.1" + target
		}
		conn, err := net.DialTimeout("tcp", target, 2*time.Second)
		if err != nil {
			fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
			os.Exit(1)
		}
		_ = conn.Close()
		fmt.Println("OK")
		os.Exit(0)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		logger.Error("failed to load configuration", "err", err)
		os.Exit(1)
	}

	// 2. Admin command: print which broker owns a given topic
	if *topicOwner != "" {
		ring := cluster.New(cfg.RingVirtualNodes)
		ring.Add(cfg.NodeID, cfg.ListenAddr)
		for _, p := range cfg.Peers {
			ring.Add(p.ID, p.Address)
		}
		owner := ring.GetOwner(*topicOwner)
		fmt.Printf("Topic %q is owned by broker: %s\n", *topicOwner, owner)
		os.Exit(0)
	}

	logger.Info("starting beacon broker",
		"node_id", cfg.NodeID,
		"listen_addr", cfg.ListenAddr,
		"peers_count", len(cfg.Peers),
		"backpressure_policy", cfg.BackpressurePolicy,
	)

	mgr := broker.NewManager(cfg, nil)
	srv := broker.NewServer(mgr, cfg, logger)

	// Set up consistent hash ring and cluster membership
	ring := cluster.New(cfg.RingVirtualNodes)
	ring.Add(cfg.NodeID, cfg.ListenAddr)
	for _, p := range cfg.Peers {
		ring.Add(p.ID, p.Address)
	}

	membership, err := cluster.NewMembership(cfg, ring, logger)
	if err != nil {
		logger.Error("failed to initialize cluster membership", "err", err)
		os.Exit(1)
	}

	router := broker.NewRouter(cfg.NodeID, ring, membership, srv, cfg, logger)
	srv.AttachRouter(router)
	defer router.Close()

	// Log owner of common topics
	logger.Info("topic owner lookup",
		"topic", "orders",
		"owner", ring.GetOwner("orders"),
	)

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("failed to bind listen address", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}

	// Listen for shutdown signals (SIGINT / SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start peer heartbeating
	if err := membership.Start(ctx); err != nil {
		logger.Warn("failed to start membership heartbeat routine", "err", err)
	}
	defer membership.Stop()

	errChan := make(chan error, 1)
	go func() {
		if err := srv.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; commencing graceful shutdown")
	case err := <-errChan:
		logger.Error("server stopped unexpectedly", "err", err)
	}

	// Graceful shutdown with timeout
	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logger.Info("broker shut down gracefully")
	case <-time.After(10 * time.Second):
		logger.Warn("graceful shutdown timed out; forcing stop")
		srv.Stop()
	}
}
