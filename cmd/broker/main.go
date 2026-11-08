// main.go is the entry point for a beacon broker node.
//
// Phase 0: wires config → metrics → broker skeleton and starts the
// Prometheus HTTP endpoint. The gRPC server is registered in Phase 1.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pranavb1234/beacon/internal/broker"
	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/metrics"
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
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	reg := prometheus.NewRegistry()
	met := metrics.New(reg)

	// Phase 0: broker is instantiated but not yet serving.
	// Phase 1 will call b.ListenAndServe(ctx).
	_ = broker.New(cfg, met)

	met.ClusterSize.Set(1)

	slog.Info("beacon broker starting",
		"node_id", cfg.NodeID,
		"grpc_addr", cfg.GRPCAddr,
		"metrics_addr", cfg.MetricsAddr,
	)

	// Prometheus metrics HTTP endpoint.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux}

	// Graceful shutdown on SIGINT / SIGTERM.
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		slog.Info("shutdown signal received")
		if err := srv.Close(); err != nil {
			slog.Error("metrics server close error", "err", err)
		}
	}()

	slog.Info("metrics server listening", "addr", cfg.MetricsAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("metrics server error", "err", err)
		os.Exit(1)
	}
}
