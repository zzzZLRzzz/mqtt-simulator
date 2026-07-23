package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"conn-conductor/pkg/action"
	act "conn-conductor/pkg/action"
	"conn-conductor/pkg/behavior"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/common"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/logging"
)

type MockMQTTClient struct {
	clientID        string
	publishDelay    time.Duration
	shouldPanic     bool
	publishCount    int
	subscribeCount  int
	disconnectCount int
	lastTopic       string
	lastPayload     any
	mu              sync.Mutex
	callOrder       []string
	connected       bool
}

func (m *MockMQTTClient) Send(payload any, target string, metadata map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callOrder = append(m.callOrder, "send:"+target)
	m.publishCount++
	m.lastTopic = target
	m.lastPayload = payload

	if m.publishDelay > 0 {
		time.Sleep(m.publishDelay)
	}

	if m.shouldPanic {
		panic("mock panic")
	}

	return nil
}

func (m *MockMQTTClient) Subscribe(target string, metadata map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.callOrder = append(m.callOrder, "subscribe:"+target)
	m.subscribeCount++
	return nil
}

func (m *MockMQTTClient) Unsubscribe(target string) error {
	return nil
}

func (m *MockMQTTClient) Disconnect() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.disconnectCount++
	m.connected = false
	return nil
}

func (m *MockMQTTClient) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connected
}

func (m *MockMQTTClient) ID() string {
	return m.clientID
}

func (m *MockMQTTClient) Connect() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected = true
	return nil
}

func (m *MockMQTTClient) StopReceiving() {
}

func (m *MockMQTTClient) GetPublishCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.publishCount
}

func (m *MockMQTTClient) GetCallOrder() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.callOrder...)
}

var _ client.Client = (*MockMQTTClient)(nil)

type mockBehavior struct{}

func (m *mockBehavior) SupportedConnectors() []string {
	return []string{config.ConnectorTypeMQTT}
}

func (m *mockBehavior) OnConnect(client client.Client) []action.Action {
	return nil
}

func (m *mockBehavior) OnMessage(client client.Client, msg common.Message) []action.Action {
	return nil
}

func (m *mockBehavior) OnTick(client client.Client, tick int64) []action.Action {
	return nil
}

func (m *mockBehavior) OnDisconnect(client client.Client) {}

var _ behavior.Behavior = (*mockBehavior)(nil)

func newTestEngine(opts ...EngineOption) *Engine {
	cfg := config.Config{
		Engine: config.EngineConfig{
			Broker: config.Broker{
				Address:   "tcp://localhost:1883",
				Keepalive: 60,
			},
			Connections:     1,
			EnableRateLimit: false,
			Connector:       config.ConnectorTypeMQTT,
		},
		Behavior: config.BehaviorConfig{
			Mode: config.BehaviorModeDeclarative,
		},
	}

	logger := logging.NewLogger(logging.LogLevelError, "test")

	e, _ := NewEngine(cfg, &mockBehavior{}, logger, opts...)
	return e
}

func TestEngine_SubmitActions_ConcurrentSafety(t *testing.T) {
	e := newTestEngine(WithWorkerCount(10), WithQueueSize(1000))
	e.startWorkerPool()
	defer e.Stop()

	const clientCount = 100
	const actionsPerClient = 10
	const expectedTotal = clientCount * actionsPerClient

	var wg sync.WaitGroup
	wg.Add(clientCount)

	for i := 0; i < clientCount; i++ {
		go func(id int) {
			defer wg.Done()
			client := &MockMQTTClient{
				clientID:  fmt.Sprintf("client-%d", id),
				connected: true,
			}

			for j := 0; j < actionsPerClient; j++ {
				actions := []act.Action{
					&act.SendAction{
						Target:   "test/topic",
						Payload:  "payload",
						Metadata: map[string]any{"qos": byte(0), "retain": false},
					},
				}
				e.SubmitActions(client, actions)
			}
		}(i)
	}

	wg.Wait()

	assert.Eventually(t, func() bool {
		return e.ProcessedCount() >= int64(expectedTotal)
	}, 3*time.Second, 50*time.Millisecond, "all actions should be processed")
}

func TestEngine_QueueDropping(t *testing.T) {
	const queueSize = 100
	e := newTestEngine(WithWorkerCount(1), WithQueueSize(queueSize))
	e.startWorkerPool()
	defer e.Stop()

	client := &MockMQTTClient{
		clientID:     "client-1",
		connected:    true,
		publishDelay: 100 * time.Millisecond,
	}

	const totalActions = 500
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < totalActions; i++ {
			actions := []act.Action{
				&act.SendAction{
					Target:   "test/topic",
					Payload:  "payload",
					Metadata: map[string]any{"qos": byte(0), "retain": false},
				},
			}
			e.SubmitActions(client, actions)
		}
	}()

	wg.Wait()

	time.Sleep(50 * time.Millisecond)

	queueLen := e.QueueLen()
	assert.LessOrEqual(t, queueLen, queueSize+10, "queue length should not significantly exceed capacity")

	assert.Eventually(t, func() bool {
		return client.GetPublishCount() > 0
	}, 2*time.Second, 100*time.Millisecond, "at least some actions should be processed")

	time.Sleep(5 * time.Second)
	processed := client.GetPublishCount()
	assert.Less(t, processed, totalActions, "some actions should be dropped due to queue full")
	assert.Greater(t, processed, 0, "at least some actions should be processed")
}

func TestEngine_SlowActionIsolation(t *testing.T) {
	e := newTestEngine(WithWorkerCount(4), WithQueueSize(100))
	e.startWorkerPool()
	defer e.Stop()

	slowClient := &MockMQTTClient{
		clientID:     "slow-client",
		connected:    true,
		publishDelay: 500 * time.Millisecond,
	}

	fastClients := make([]*MockMQTTClient, 20)
	for i := 0; i < 20; i++ {
		fastClients[i] = &MockMQTTClient{
			clientID:  fmt.Sprintf("fast-client-%d", i),
			connected: true,
		}
	}

	e.SubmitActions(slowClient, []act.Action{
		&act.SendAction{Target: "slow/topic", Payload: "slow", Metadata: map[string]any{"qos": byte(0), "retain": false}},
	})

	for _, fc := range fastClients {
		e.SubmitActions(fc, []act.Action{
			&act.SendAction{Target: "fast/topic", Payload: "fast", Metadata: map[string]any{"qos": byte(0), "retain": false}},
		})
	}

	time.Sleep(200 * time.Millisecond)

	fastProcessed := 0
	for _, fc := range fastClients {
		fastProcessed += fc.GetPublishCount()
	}

	assert.Greater(t, fastProcessed, 10, "most fast actions should be processed before slow action finishes")
}

func TestEngine_WorkerPanicRecovery(t *testing.T) {
	e := newTestEngine(WithWorkerCount(1), WithQueueSize(10))
	e.startWorkerPool()
	defer e.Stop()

	panicClient := &MockMQTTClient{
		clientID:    "panic-client",
		connected:   true,
		shouldPanic: true,
	}

	normalClient := &MockMQTTClient{
		clientID:  "normal-client",
		connected: true,
	}

	e.SubmitActions(panicClient, []act.Action{
		&act.SendAction{Target: "panic/topic", Payload: "panic", Metadata: map[string]any{"qos": byte(0), "retain": false}},
	})

	time.Sleep(100 * time.Millisecond)

	e.SubmitActions(normalClient, []act.Action{
		&act.SendAction{Target: "normal/topic", Payload: "normal", Metadata: map[string]any{"qos": byte(0), "retain": false}},
	})

	assert.Eventually(t, func() bool {
		return normalClient.GetPublishCount() == 1
	}, 2*time.Second, 100*time.Millisecond, "normal action should be processed eventually after panic")
}

func TestEngine_OrderPreservation(t *testing.T) {
	e := newTestEngine(WithWorkerCount(10), WithQueueSize(500))
	e.startWorkerPool()
	defer e.Stop()

	client := &MockMQTTClient{
		clientID:  "order-client",
		connected: true,
	}

	const actionCount = 100
	for i := 0; i < actionCount; i++ {
		topic := fmt.Sprintf("topic/%d", i)
		e.SubmitActions(client, []act.Action{
			&act.SendAction{Target: topic, Payload: "payload", Metadata: map[string]any{"qos": byte(0), "retain": false}},
		})
	}

	assert.Eventually(t, func() bool {
		return e.ProcessedCount() >= int64(actionCount)
	}, 3*time.Second, 50*time.Millisecond, "all actions should be processed")

	callOrder := client.GetCallOrder()

	expectedOrder := make([]string, actionCount)
	for i := 0; i < actionCount; i++ {
		expectedOrder[i] = fmt.Sprintf("send:topic/%d", i)
	}

	assert.Equal(t, expectedOrder, callOrder, "actions should be processed in order via hash routing")
}

func BenchmarkEngine_SubmitThroughput(b *testing.B) {
	e := newTestEngine(WithWorkerCount(10), WithQueueSize(5000))
	e.startWorkerPool()
	defer e.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client := &MockMQTTClient{
			clientID:  fmt.Sprintf("bench-%d", time.Now().UnixNano()),
			connected: true,
		}
		action := []act.Action{&act.SendAction{Target: "test"}}
		for pb.Next() {
			e.SubmitActions(client, action)
		}
	})
	b.StopTimer()

	deadline := time.Now().Add(10 * time.Second)
	for e.ProcessedCount() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

func BenchmarkEngine_EndToEndThroughput(b *testing.B) {
	e := newTestEngine(WithWorkerCount(10), WithQueueSize(5000))
	e.startWorkerPool()
	defer e.Stop()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		client := &MockMQTTClient{
			clientID:  fmt.Sprintf("bench-%d", time.Now().UnixNano()),
			connected: true,
		}
		action := []act.Action{&act.SendAction{Target: "test"}}
		for pb.Next() {
			e.SubmitActions(client, action)
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for e.ProcessedCount() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	b.StopTimer()
}
