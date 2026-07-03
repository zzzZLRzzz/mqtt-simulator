package behavior

import (
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"mqtt-simulator/pkg/common"
	"mqtt-simulator/pkg/config"
	"mqtt-simulator/pkg/logging"
)

type mockClientContext struct {
	clientID string
}

func (m *mockClientContext) Publish(topic string, qos byte, retain bool, payload interface{}) error {
	return nil
}

func (m *mockClientContext) Subscribe(topic string, qos byte) error {
	return nil
}

func (m *mockClientContext) Unsubscribe(topic string) error {
	return nil
}

func (m *mockClientContext) Disconnect() error {
	return nil
}

func (m *mockClientContext) IsConnected() bool {
	return true
}

func (m *mockClientContext) ClientID() string {
	return m.clientID
}

type mockMessage struct {
	topic   string
	payload []byte
}

func (m *mockMessage) Duplicate() bool {
	return false
}

func (m *mockMessage) Qos() byte {
	return 0
}

func (m *mockMessage) Retained() bool {
	return false
}

func (m *mockMessage) Topic() string {
	return m.topic
}

func (m *mockMessage) Payload() []byte {
	return m.payload
}

func (m *mockMessage) Ack() {
}

func (m *mockMessage) MessageID() uint16 {
	return 0
}

var _ mqtt.Message = (*mockMessage)(nil)

func TestDeclarativeBehavior_OnConnect(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")

	tests := []struct {
		name        string
		config      config.BehaviorConfig
		clientID    string
		wantActions int
		wantTypes   []string
	}{
		{
			name: "empty on_connect",
			config: config.BehaviorConfig{
				Mode:      config.BehaviorModeDeclarative,
				OnConnect: []config.BehaviorAction{},
			},
			clientID:    "client1",
			wantActions: 0,
			wantTypes:   []string{},
		},
		{
			name: "single subscribe",
			config: config.BehaviorConfig{
				Mode: config.BehaviorModeDeclarative,
				OnConnect: []config.BehaviorAction{
					{
						Subscribe: &config.SubscribeActionConfig{
							Topic: "test/topic",
							QoS:   1,
						},
					},
				},
			},
			clientID:    "client1",
			wantActions: 1,
			wantTypes:   []string{"SubscribeAction"},
		},
		{
			name: "multiple actions",
			config: config.BehaviorConfig{
				Mode: config.BehaviorModeDeclarative,
				OnConnect: []config.BehaviorAction{
					{
						Subscribe: &config.SubscribeActionConfig{
							Topic: "topic1",
							QoS:   0,
						},
					},
					{
						Publish: &config.PublishActionConfig{
							Topic:   "topic2",
							Payload: "hello",
							QoS:     1,
						},
					},
				},
			},
			clientID:    "client1",
			wantActions: 2,
			wantTypes:   []string{"SubscribeAction", "PublishAction"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewDeclarativeBehavior(tt.config, logger)
			ctx := &mockClientContext{clientID: tt.clientID}
			actions := b.OnConnect(ctx)

			if len(actions) != tt.wantActions {
				t.Errorf("OnConnect() got %d actions, want %d", len(actions), tt.wantActions)
				return
			}

			for i, action := range actions {
				switch action.(type) {
				case common.SubscribeAction:
					if tt.wantTypes[i] != "SubscribeAction" {
						t.Errorf("action[%d] = SubscribeAction, want %s", i, tt.wantTypes[i])
					}
				case common.PublishAction:
					if tt.wantTypes[i] != "PublishAction" {
						t.Errorf("action[%d] = PublishAction, want %s", i, tt.wantTypes[i])
					}
				case common.UnsubscribeAction:
					if tt.wantTypes[i] != "UnsubscribeAction" {
						t.Errorf("action[%d] = UnsubscribeAction, want %s", i, tt.wantTypes[i])
					}
				case common.DisconnectAction:
					if tt.wantTypes[i] != "DisconnectAction" {
						t.Errorf("action[%d] = DisconnectAction, want %s", i, tt.wantTypes[i])
					}
				}
			}
		})
	}
}

func TestDeclarativeBehavior_OnMessage(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")

	tests := []struct {
		name        string
		config      config.BehaviorConfig
		clientID    string
		msgTopic    string
		msgPayload  string
		wantActions int
	}{
		{
			name: "echo payload",
			config: config.BehaviorConfig{
				Mode: config.BehaviorModeDeclarative,
				OnMessage: []config.BehaviorAction{
					{
						Publish: &config.PublishActionConfig{
							Topic:   "response/{{.MessageTopic}}",
							Payload: "{{.MessagePayload}}",
							QoS:     0,
						},
					},
				},
			},
			clientID:    "client1",
			msgTopic:    "request/test",
			msgPayload:  "hello world",
			wantActions: 1,
		},
		{
			name: "forward to topic with client id",
			config: config.BehaviorConfig{
				Mode: config.BehaviorModeDeclarative,
				OnMessage: []config.BehaviorAction{
					{
						Publish: &config.PublishActionConfig{
							Topic:   "devices/{{.ClientID}}/data",
							Payload: "{{.MessagePayload}}",
							QoS:     1,
						},
					},
				},
			},
			clientID:    "device-001",
			msgTopic:    "sensor/data",
			msgPayload:  "{\"temp\":25}",
			wantActions: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewDeclarativeBehavior(tt.config, logger)
			ctx := &mockClientContext{clientID: tt.clientID}
			msg := &mockMessage{topic: tt.msgTopic, payload: []byte(tt.msgPayload)}
			actions := b.OnMessage(ctx, msg)

			if len(actions) != tt.wantActions {
				t.Errorf("OnMessage() got %d actions, want %d", len(actions), tt.wantActions)
				return
			}

			if len(actions) > 0 {
				publishAction, ok := actions[0].(common.PublishAction)
				if ok {
					t.Logf("PublishAction topic: %s", publishAction.Topic)
					t.Logf("PublishAction payload: %s", publishAction.Payload)
				}
			}
		})
	}
}

func TestDeclarativeBehavior_OnTick(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")

	tests := []struct {
		name        string
		config      config.BehaviorConfig
		clientID    string
		wantActions int
	}{
		{
			name: "periodic publish",
			config: config.BehaviorConfig{
				Mode: config.BehaviorModeDeclarative,
				OnTimer: []config.BehaviorAction{
					{
						Interval: 1,
						Publish: &config.PublishActionConfig{
							Topic:   "heartbeat/{{.ClientID}}",
							Payload: "alive",
							QoS:     0,
							Retain:  false,
						},
					},
				},
			},
			clientID:    "client1",
			wantActions: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewDeclarativeBehavior(tt.config, logger)
			ctx := &mockClientContext{clientID: tt.clientID}
			actions := b.OnTick(ctx, 0)

			if len(actions) != tt.wantActions {
				t.Errorf("OnTick() got %d actions, want %d", len(actions), tt.wantActions)
				return
			}
		})
	}
}

func TestDeclarativeBehavior_TemplateRendering(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")

	b := NewDeclarativeBehavior(config.BehaviorConfig{
		Mode: config.BehaviorModeDeclarative,
		OnMessage: []config.BehaviorAction{
			{
				Publish: &config.PublishActionConfig{
					Topic:   "response/{{.MessageTopic}}",
					Payload: "from {{.ClientID}}: {{.MessagePayload}}",
					QoS:     0,
				},
			},
		},
	}, logger)

	ctx := &mockClientContext{clientID: "my-client"}
	msg := &mockMessage{topic: "test/topic", payload: []byte("hello")}

	actions := b.OnMessage(ctx, msg)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}

	publishAction, ok := actions[0].(common.PublishAction)
	if !ok {
		t.Fatalf("expected PublishAction, got %T", actions[0])
	}

	expectedTopic := "response/test/topic"
	if publishAction.Topic != expectedTopic {
		t.Errorf("topic = %s, want %s", publishAction.Topic, expectedTopic)
	}

	expectedPayload := "from my-client: hello"
	if publishAction.Payload != expectedPayload {
		t.Errorf("payload = %s, want %s", publishAction.Payload, expectedPayload)
	}
}

func TestDeclarativeBehavior_Unsubscribe(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")

	b := NewDeclarativeBehavior(config.BehaviorConfig{
		Mode: config.BehaviorModeDeclarative,
		OnConnect: []config.BehaviorAction{
			{
				Unsubscribe: func() *string { s := "old/topic"; return &s }(),
			},
		},
	}, logger)

	ctx := &mockClientContext{clientID: "client1"}
	actions := b.OnConnect(ctx)

	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}

	if _, ok := actions[0].(common.UnsubscribeAction); !ok {
		t.Errorf("expected UnsubscribeAction, got %T", actions[0])
	}
}

func TestDeclarativeBehavior_Disconnect(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")

	b := NewDeclarativeBehavior(config.BehaviorConfig{
		Mode: config.BehaviorModeDeclarative,
		OnMessage: []config.BehaviorAction{
			{
				Disconnect: true,
			},
		},
	}, logger)

	ctx := &mockClientContext{clientID: "client1"}
	msg := &mockMessage{topic: "cmd/disconnect", payload: []byte("")}
	actions := b.OnMessage(ctx, msg)

	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}

	if _, ok := actions[0].(common.DisconnectAction); !ok {
		t.Errorf("expected DisconnectAction, got %T", actions[0])
	}
}
