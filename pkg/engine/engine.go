package engine

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"conn-conductor/pkg/action"
	"conn-conductor/pkg/behavior"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/connector"
	"conn-conductor/pkg/generator"
	"conn-conductor/pkg/logging"
	"conn-conductor/pkg/metrics"
)

const DefaultWorkerCount = 10
const DefaultQueueSize = 1000

type ActionWithContext struct {
	Actions []action.Action
	Client  client.Client
}

type EngineOption func(*Engine)

func WithWorkerCount(count int) EngineOption {
	return func(e *Engine) {
		if count > 0 {
			e.workerCount = count
		}
	}
}

func WithQueueSize(size int) EngineOption {
	return func(e *Engine) {
		if size > 0 {
			e.queueSize = size
		}
	}
}

func WithConnectorFactory(factory connector.ConnectorFactory) EngineOption {
	return func(e *Engine) {
		e.connectorFactory = factory
	}
}

type Engine struct {
	config            config.Config
	pool              *connector.ConnectionPool
	logger            *logging.Logger
	behavior          behavior.Behavior
	msgCount          int64
	processedCount    int64
	wg                sync.WaitGroup
	actionQueues      []chan ActionWithContext
	workerCount       int
	queueSize         int
	connectLimiter    *rate.Limiter
	sendLimiter       *rate.Limiter
	subscribeLimiter  *rate.Limiter
	disconnectLimiter *rate.Limiter
	globalTicker      *time.Ticker
	globalTick        int64
	credGen           generator.CredentialGenerator
	connectorFactory  connector.ConnectorFactory
	ctx               context.Context
	cancel            context.CancelFunc
}

func NewEngine(cfg config.Config, beh behavior.Behavior, logger *logging.Logger, opts ...EngineOption) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		config:       cfg,
		logger:       logger,
		behavior:     beh,
		workerCount:  DefaultWorkerCount,
		queueSize:    DefaultQueueSize,
		actionQueues: make([]chan ActionWithContext, DefaultWorkerCount),
		credGen:      generator.NewDefaultCredentialGenerator(cfg.Engine.Credentials),
		ctx:          ctx,
		cancel:       cancel,
	}

	for _, opt := range opts {
		opt(e)
	}

	if e.connectorFactory == nil {
		e.connectorFactory = connector.NewMQTTConnectorFactory(logger, cfg.Engine.Broker)
	}

	e.actionQueues = make([]chan ActionWithContext, e.workerCount)
	for i := 0; i < e.workerCount; i++ {
		e.actionQueues[i] = make(chan ActionWithContext, e.queueSize)
	}

	e.initRateLimiters(cfg.Engine.RateLimits, logger)

	return e
}

func (e *Engine) initRateLimiters(rateLimits config.RateLimitsConfig, logger *logging.Logger) {
	if rateLimits.Connect.Rate > 0 {
		burst := rateLimits.Connect.Burst
		if burst <= 0 {
			burst = rateLimits.Connect.Rate
		}
		e.connectLimiter = rate.NewLimiter(rate.Limit(rateLimits.Connect.Rate), burst)
		logger.Info("Connect limiter created: rate=%d, burst=%d", rateLimits.Connect.Rate, burst)
	}

	if rateLimits.Subscribe.Rate > 0 {
		burst := rateLimits.Subscribe.Burst
		if burst <= 0 {
			burst = 1
		}
		e.subscribeLimiter = rate.NewLimiter(rate.Limit(rateLimits.Subscribe.Rate), burst)
		logger.Info("Subscribe limiter created: rate=%d, burst=%d", rateLimits.Subscribe.Rate, burst)
	}

	if rateLimits.Send.Rate > 0 {
		burst := rateLimits.Send.Burst
		if burst <= 0 {
			burst = rateLimits.Send.Rate
		}
		e.sendLimiter = rate.NewLimiter(rate.Limit(rateLimits.Send.Rate), burst)
		logger.Info("Send limiter created: rate=%d, burst=%d", rateLimits.Send.Rate, burst)
	}

	if rateLimits.Disconnect.Rate > 0 {
		burst := rateLimits.Disconnect.Burst
		if burst <= 0 {
			burst = rateLimits.Disconnect.Rate
		}
		e.disconnectLimiter = rate.NewLimiter(rate.Limit(rateLimits.Disconnect.Rate), burst)
		logger.Info("Disconnect limiter created: rate=%d, burst=%d", rateLimits.Disconnect.Rate, burst)
	}
}

func (e *Engine) queueIndex(clientID string) int {
	h := fnv.New32a()
	h.Write([]byte(clientID))
	return int(h.Sum32()) % e.workerCount
}

func (e *Engine) SubmitActions(client client.Client, actions []action.Action) {
	if len(actions) == 0 {
		return
	}
	queueIdx := e.queueIndex(client.ID())
	select {
	case e.actionQueues[queueIdx] <- ActionWithContext{Actions: actions, Client: client}:
	default:
		e.logger.Warn("[%s] action queue full, dropping %d actions", client.ID(), len(actions))
	}
}

func (e *Engine) startWorkerPool() {
	for i := 0; i < e.workerCount; i++ {
		e.wg.Add(1)
		go func(queueIdx int) {
			defer e.wg.Done()
			for item := range e.actionQueues[queueIdx] {
				func() {
					defer func() {
						if r := recover(); r != nil {
							e.logger.Error("[worker-%d] panic recovered: %v", queueIdx, r)
						}
					}()
					e.executeActions(item.Client, item.Actions)
				}()
			}
		}(i)
	}
}

func (e *Engine) ProcessedCount() int64 {
	return atomic.LoadInt64(&e.processedCount)
}

func (e *Engine) QueueLen() int {
	total := 0
	for _, q := range e.actionQueues {
		total += len(q)
	}
	return total
}

func (e *Engine) executeActions(client client.Client, actions []action.Action) {
	for _, action := range actions {
		e.executeAction(client, action)
		atomic.AddInt64(&e.processedCount, 1)
	}
}

func (e *Engine) executeAction(client client.Client, act action.Action) {
	switch act.(type) {
	case *action.SendAction:
		if e.sendLimiter != nil {
			if err := e.sendLimiter.Wait(e.ctx); err != nil {
				e.logger.Error("[%s] send rate limit error: %v", client.ID(), err)
				return
			}
		}
	case *action.SubscribeAction:
		if e.subscribeLimiter != nil {
			if err := e.subscribeLimiter.Wait(e.ctx); err != nil {
				e.logger.Error("[%s] subscribe rate limit error: %v", client.ID(), err)
				return
			}
		}
	case *action.DisconnectAction:
		if e.disconnectLimiter != nil {
			if err := e.disconnectLimiter.Wait(e.ctx); err != nil {
				e.logger.Error("[%s] disconnect rate limit error: %v", client.ID(), err)
				return
			}
		}
	}

	start := time.Now()
	err := act.Execute(client)
	latency := time.Since(start)

	if err != nil {
		e.logger.Error("[%s] action failed: %v", client.ID(), err)
		metrics.MessagesFailed.Inc()
		return
	}

	if sa, ok := act.(*action.SendAction); ok {
		atomic.AddInt64(&e.msgCount, 1)
		metrics.MessagesPublished.WithLabelValues(sa.Target).Inc()
		metrics.PublishLatency.Observe(latency.Seconds())
	}
}

func (e *Engine) connectAll() {
	var connectWg sync.WaitGroup
	connectionCount := e.config.Engine.Connections

	for i := 1; i <= connectionCount; i++ {
		connectWg.Add(1)
		go func(index int) {
			defer connectWg.Done()

			if e.connectLimiter != nil {
				if err := e.connectLimiter.Wait(e.ctx); err != nil {
					e.logger.Error("[client-%d] connect rate limit error: %v", index, err)
					return
				}
			}

			creds := config.Creds{
				ClientID: e.credGen.GenerateClientID(index),
				Username: e.credGen.GenerateUsername(index),
				Password: e.credGen.GeneratePassword(index),
			}

			if e.connectorFactory == nil {
				e.logger.Error("[client-%d] connector factory is nil", index)
				return
			}

			client := e.connectorFactory.CreateClient(index, creds, e.behavior, e.handleAction)
			if err := client.Connect(); err != nil {
				e.logger.Error("[%s] connect failed: %v", creds.ClientID, err)
				metrics.IncConnectionFailed()
				return
			}

			e.pool.Add(client)
			metrics.IncConnection()
			e.logger.Info("[%s] connected successfully", creds.ClientID)

		}(i)
	}

	connectWg.Wait()
	e.logger.Info("ConnectAll completed: %d/%d clients", e.pool.Count(), connectionCount)
}

func (e *Engine) handleAction(client client.Client, actions []action.Action) {
	e.SubmitActions(client, actions)
}

func (e *Engine) startGlobalTicker() {
	e.globalTicker = time.NewTicker(1 * time.Second)

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-e.ctx.Done():
				return
			case <-e.globalTicker.C:
				tick := atomic.AddInt64(&e.globalTick, 1)
				e.broadcastTimerTick(tick)
			}
		}
	}()
}

func (e *Engine) broadcastTimerTick(tick int64) {
	if e.pool == nil {
		return
	}
	clients := e.pool.All()
	for _, c := range clients {
		if !c.IsConnected() {
			continue
		}
		go func(cl client.Client) {
			actions := e.behavior.OnTick(cl, tick)
			e.SubmitActions(cl, actions)
		}(c)
	}
}

func (e *Engine) stopGlobalTicker() {
	if e.globalTicker != nil {
		e.globalTicker.Stop()
		e.globalTicker = nil
	}
}

func (e *Engine) Run() error {
	e.pool = connector.NewConnectionPool()

	e.logger.Info("Starting behavior with %d connections...", e.config.Engine.Connections)

	e.startWorkerPool()

	e.connectAll()

	e.startGlobalTicker()

	e.logger.Info("Behavior running. Press Ctrl+C to stop.")

	return nil
}

func (e *Engine) Stop() {
	e.cancel()

	e.stopGlobalTicker()

	if e.pool != nil {
		e.pool.StopAllClients()
	}

	for _, queue := range e.actionQueues {
		close(queue)
	}

	e.wg.Wait()

	if e.pool != nil {
		e.logger.Info("Disconnecting all clients...")
		e.pool.DisconnectAll()
	}

	e.logger.Info("Behavior stopped. Messages published: %d", e.msgCount)
	e.logger.Info("Total messages received: %d", metrics.GetReceivedCount())
}
