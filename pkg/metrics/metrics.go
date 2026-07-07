package metrics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"mqtt-simulator/pkg/logging"
)

var (
	ConnectionsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "mqtt_connections_total",
			Help: "Total number of MQTT connections",
		},
	)

	ConnectionsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "mqtt_connections_active",
			Help: "Number of active MQTT connections",
		},
	)

	ConnectionsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "mqtt_connections_failed",
			Help: "Number of failed MQTT connections",
		},
	)

	MessagesPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mqtt_messages_published",
			Help: "Number of published MQTT messages",
		},
		[]string{"topic"},
	)

	MessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mqtt_messages_received",
			Help: "Number of received MQTT messages",
		},
		[]string{"topic"},
	)

	MessagesFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "mqtt_messages_failed",
			Help: "Number of failed MQTT messages",
		},
	)

	PublishLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "mqtt_publish_latency_seconds",
			Help:    "Publish latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	ConnectionAttempts = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "mqtt_connection_attempts",
			Help: "Number of connection attempts",
		},
	)
)

type MetricsServer struct {
	server *http.Server
	logger *logging.Logger
}

func StartServer(port int, logger *logging.Logger) *MetricsServer {
	if port <= 0 {
		return nil
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		ConnectionsTotal,
		ConnectionsActive,
		ConnectionsFailed,
		MessagesPublished,
		MessagesReceived,
		MessagesFailed,
		PublishLatency,
		ConnectionAttempts,
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	addr := fmt.Sprintf(":%d", port)

	s := &MetricsServer{
		logger: logger,
		server: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}

	logger.Info("Prometheus metrics server starting on %s", addr)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server error: %v", err)
		}
	}()

	return s
}

func (s *MetricsServer) Stop() {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.server.Shutdown(ctx); err != nil {
			s.logger.Error("Metrics server shutdown error: %v", err)
		} else {
			s.logger.Info("Metrics server shutdown completed")
		}
	}
}

func IncConnection() {
	ConnectionsTotal.Inc()
	ConnectionsActive.Inc()
	ConnectionAttempts.Inc()
}

func DecConnection() {
	ConnectionsActive.Dec()
}

func IncConnectionFailed() {
	ConnectionsFailed.Inc()
}
