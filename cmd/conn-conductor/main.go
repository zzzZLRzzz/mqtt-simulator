package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"conn-conductor/pkg/behavior"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/engine"
	"conn-conductor/pkg/logging"
	"conn-conductor/pkg/metrics"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	logger := logging.NewLogger(logging.LogLevelInfo, "conn-conductor")

	logger.Printf("Loading configuration from %s...", *configPath)
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	logLevel := logging.LogLevelInfo
	if cfg.LogLevel != "" {
		logLevel = logging.LogLevel(cfg.LogLevel)
	}
	logger = logging.NewLogger(logLevel, "conn-conductor")
	logger.Info("Log level set to: %s", logLevel)

	var metricsServer *metrics.MetricsServer
	if cfg.Metrics.Enable && cfg.Metrics.PrometheusPort > 0 {
		metricsServer = metrics.StartServer(cfg.Metrics.PrometheusPort, logger)
	}

	beh := behavior.NewBehavior(cfg.Behavior, logger)

	sim, err := engine.NewEngine(*cfg, beh, logger)
	if err != nil {
		logger.Fatalf("Failed to create engine: %v", err)
	}

	if err := sim.Run(); err != nil {
		logger.Fatalf("Failed to start simulator: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.Printf("Received signal: %v, stopping simulator...", sig)

	if metricsServer != nil {
		metricsServer.Stop()
	}

	sim.Stop()
	logger.Println("Simulator stopped successfully")
}
