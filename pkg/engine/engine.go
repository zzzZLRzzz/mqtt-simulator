package engine

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"mqtt-simulator/pkg/behavior"
	"mqtt-simulator/pkg/common"
	"mqtt-simulator/pkg/config"
	"mqtt-simulator/pkg/connector"
	"mqtt-simulator/pkg/generator"
	"mqtt-simulator/pkg/logging"
	"mqtt-simulator/pkg/metrics"
)

const DefaultWorkerCount = 10

type ActionWithContext struct {
	Actions []common.Action
	Client  common.ClientContext
}

type Engine struct {
	config            config.Config
	pool              *connector.ConnectionPool
	logger            *logging.Logger
	behavior          behavior.Behavior
	msgCount          int64
	running           bool
	mu                sync.Mutex
	wg                sync.WaitGroup
	actionQueues      []chan ActionWithContext
	workerCount       int
	connectLimiter    *rate.Limiter
	publishLimiter    *rate.Limiter
	subscribeLimiter  *rate.Limiter
	disconnectLimiter *rate.Limiter
	globalTicker      *time.Ticker
	globalTick        int64
	stopChan          chan struct{}
	credGen           generator.CredentialGenerator
}

func NewEngine(cfg config.Config, beh behavior.Behavior, logger *logging.Logger) *Engine {
	e := &Engine{
		config:       cfg,
		logger:       logger,
		behavior:     beh,
		workerCount:  DefaultWorkerCount,
		actionQueues: make([]chan ActionWithContext, DefaultWorkerCount),
		credGen:      generator.NewDefaultCredentialGenerator(cfg.Engine.Credentials),
	}

	for i := 0; i < DefaultWorkerCount; i++ {
		e.actionQueues[i] = make(chan ActionWithContext, 1000)
	}

	if cfg.Engine.EnableRateLimit {
		rateLimits := cfg.Engine.RateLimits

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

		if rateLimits.Publish.Rate > 0 {
			burst := rateLimits.Publish.Burst
			if burst <= 0 {
				burst = rateLimits.Publish.Rate
			}
			e.publishLimiter = rate.NewLimiter(rate.Limit(rateLimits.Publish.Rate), burst)
			logger.Info("Publish limiter created: rate=%d, burst=%d", rateLimits.Publish.Rate, burst)
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

	return e
}

func (e *Engine) queueIndex(clientID string) int {
	h := fnv.New32a()
	h.Write([]byte(clientID))
	return int(h.Sum32()) % e.workerCount
}

func (e *Engine) SubmitActions(ctx common.ClientContext, actions []common.Action) {
	if len(actions) == 0 {
		return
	}
	queueIdx := e.queueIndex(ctx.ClientID())
	select {
	case e.actionQueues[queueIdx] <- ActionWithContext{Actions: actions, Client: ctx}:
	default:
		e.logger.Warn("[%s] action queue full, dropping %d actions", ctx.ClientID(), len(actions))
	}
}

func (e *Engine) startWorkerPool() {
	for i := 0; i < e.workerCount; i++ {
		e.wg.Add(1)
		go func(queueIdx int) {
			defer e.wg.Done()
			for item := range e.actionQueues[queueIdx] {
				e.executeActions(item.Client, item.Actions)
			}
		}(i)
	}
}

func (e *Engine) executeActions(ctx common.ClientContext, actions []common.Action) {
	for _, action := range actions {
		switch a := action.(type) {
		case common.PublishAction:
			e.executePublishAction(ctx, a)
		case common.SubscribeAction:
			e.executeSubscribeAction(ctx, a)
		case common.UnsubscribeAction:
			e.executeUnsubscribeAction(ctx, a)
		case common.DisconnectAction:
			e.executeDisconnectAction(ctx)
		}
	}
}

func (e *Engine) executePublishAction(ctx common.ClientContext, action common.PublishAction) {
	if e.publishLimiter != nil {
		if err := e.publishLimiter.Wait(context.Background()); err != nil {
			e.logger.Error("[%s] publish rate limit error: %v", ctx.ClientID(), err)
			return
		}
	}

	start := time.Now()
	err := ctx.Publish(action.Topic, action.QoS, action.Retain, action.Payload)
	latency := time.Since(start)

	if err != nil {
		e.logger.Error("[%s] failed to publish: %v", ctx.ClientID(), err)
		metrics.MessagesFailed.Inc()
		return
	}

	atomic.AddInt64(&e.msgCount, 1)
	metrics.MessagesPublished.WithLabelValues(action.Topic).Inc()
	metrics.PublishLatency.Observe(latency.Seconds())

	if e.logger.IsDebug() {
		var payloadStr string
		switch v := action.Payload.(type) {
		case []byte:
			payloadStr = string(v)
		case string:
			payloadStr = v
		default:
			payloadStr = "<binary>"
		}
		e.logger.Debug("[%s] published to %s: %s", ctx.ClientID(), action.Topic, payloadStr)
	} else if e.logger.IsInfo() {
		e.logger.Info("[%s] published to %s", ctx.ClientID(), action.Topic)
	}
}

func (e *Engine) executeSubscribeAction(ctx common.ClientContext, action common.SubscribeAction) {
	if e.subscribeLimiter != nil {
		if err := e.subscribeLimiter.Wait(context.Background()); err != nil {
			e.logger.Error("[%s] subscribe rate limit error: %v", ctx.ClientID(), err)
			return
		}
	}

	if err := ctx.Subscribe(action.Topic, action.QoS); err != nil {
		e.logger.Error("[%s] failed to subscribe to %s: %v", ctx.ClientID(), action.Topic, err)
	}
}

func (e *Engine) executeUnsubscribeAction(ctx common.ClientContext, action common.UnsubscribeAction) {
	if err := ctx.Unsubscribe(action.Topic); err != nil {
		e.logger.Error("[%s] failed to unsubscribe from %s: %v", ctx.ClientID(), action.Topic, err)
	}
}

func (e *Engine) executeDisconnectAction(ctx common.ClientContext) {
	if e.disconnectLimiter != nil {
		if err := e.disconnectLimiter.Wait(context.Background()); err != nil {
			e.logger.Error("[%s] disconnect rate limit error: %v", ctx.ClientID(), err)
			return
		}
	}
	if err := ctx.Disconnect(); err != nil {
		e.logger.Error("[%s] failed to disconnect: %v", ctx.ClientID(), err)
	}
	e.behavior.OnDisconnect(ctx)
}

func (e *Engine) connectAll() {
	var connectWg sync.WaitGroup
	connectionCount := e.config.Engine.Connections

	for i := 1; i <= connectionCount; i++ {
		connectWg.Add(1)
		go func(index int) {
			defer connectWg.Done()

			if e.connectLimiter != nil {
				if err := e.connectLimiter.Wait(context.Background()); err != nil {
					e.logger.Error("[client-%d] connect rate limit error: %v", index, err)
					return
				}
			}

			creds := config.Creds{
				ClientID: e.credGen.GenerateClientID(index),
				Username: e.credGen.GenerateUsername(index),
				Password: e.credGen.GeneratePassword(index),
			}

			client := connector.NewMQTTClient(e.logger, e.config.Engine.Broker, index, creds, e.behavior, e.handleAction)
			if err := client.Connect(); err != nil {
				e.logger.Error("[%s] connect failed: %v", creds.ClientID, err)
				metrics.IncConnectionFailed()
				return
			}

			e.pool.Add(client)
			metrics.IncConnection()
			e.logger.Info("[%s] connected successfully", creds.ClientID)

			actions := e.behavior.OnConnect(client)
			e.SubmitActions(client, actions)

		}(i)
	}

	connectWg.Wait()
	e.logger.Info("ConnectAll completed: %d/%d clients", e.pool.Count(), connectionCount)
}

func (e *Engine) handleAction(ctx common.ClientContext, actions []common.Action) {
	e.SubmitActions(ctx, actions)
}

func (e *Engine) startGlobalTicker() {
	e.globalTicker = time.NewTicker(1 * time.Second)
	e.stopChan = make(chan struct{})

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-e.stopChan:
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
	for _, client := range clients {
		if !client.IsConnected() {
			continue
		}
		go func(c common.ClientContext) {
			actions := e.behavior.OnTick(c, tick)
			e.SubmitActions(c, actions)
		}(client)
	}
}

func (e *Engine) stopGlobalTicker() {
	if e.globalTicker != nil {
		e.globalTicker.Stop()
		e.globalTicker = nil
	}
	if e.stopChan != nil {
		close(e.stopChan)
		e.stopChan = nil
	}
}

func (e *Engine) Run() error {
	e.mu.Lock()
	e.running = true
	e.mu.Unlock()

	e.pool = connector.NewConnectionPool()

	e.logger.Info("Starting behavior with %d connections...", e.config.Engine.Connections)

	e.startWorkerPool()

	e.connectAll()

	e.startGlobalTicker()

	e.logger.Info("Behavior running. Press Ctrl+C to stop.")

	return nil
}

func (e *Engine) Stop() {
	e.mu.Lock()
	e.running = false
	e.mu.Unlock()

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

	e.logger.Info("Behavior stopped. Total messages: %d", e.msgCount)
}
