package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"mqtt-simulator/pkg/behavior"
	"mqtt-simulator/pkg/config"
	"mqtt-simulator/pkg/engine"
	"mqtt-simulator/pkg/logging"
	"mqtt-simulator/pkg/metrics"
)

type Simulator interface {
	Run() error
	Stop()
}

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	logger := logging.NewLogger(logging.LogLevelInfo, "mqtt-simulator")

	logger.Printf("Loading configuration from %s...", *configPath)
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	logLevel := logging.LogLevelInfo
	if cfg.LogLevel != "" {
		logLevel = logging.LogLevel(cfg.LogLevel)
	}
	logger = logging.NewLogger(logLevel, "mqtt-simulator")
	logger.Info("Log level set to: %s", logLevel)

	var metricsServer *metrics.MetricsServer
	if cfg.Metrics.Enable && cfg.Metrics.PrometheusPort > 0 {
		metricsServer = metrics.StartServer(cfg.Metrics.PrometheusPort, logger)
	}

	var sim Simulator
	var beh behavior.Behavior

	if cfg.Behavior.Mode == config.BehaviorModeCustom {
		beh = behavior.NewUSPBehavior(logger)
	} else {
		beh = behavior.NewDeclarativeBehavior(cfg.Behavior, logger)
	}

	sim = engine.NewEngine(*cfg, beh, logger)

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
