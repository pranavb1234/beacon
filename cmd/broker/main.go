package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pranavb1234/beacon/internal/broker"
	"github.com/pranavb1234/beacon/internal/config"
)

func main() {
	cfgPath := flag.String("config", "", "path to YAML config file (optional; env vars override)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		logger.Error("failed to load configuration", "err", err)
		os.Exit(1)
	}

	logger.Info("starting beacon broker",
		"node_id", cfg.NodeID,
		"listen_addr", cfg.ListenAddr,
		"backpressure_policy", cfg.BackpressurePolicy,
	)

	mgr := broker.NewManager(cfg, nil)
	srv := broker.NewServer(mgr, cfg, logger)

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("failed to bind listen address", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}

	errChan := make(chan error, 1)
	go func() {
		if err := srv.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	// Listen for shutdown signals (SIGINT / SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	fmt.Println("broker")
}
