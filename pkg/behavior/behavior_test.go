package behavior

import (
	"testing"

	act "conn-conductor/pkg/action"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/common"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/logging"
)

type mockClientContext struct {
	clientID string
}

func (m *mockClientContext) Send(payload any, target string, metadata map[string]any) error {
	return nil
}

func (m *mockClientContext) Subscribe(target string, metadata map[string]any) error {
	return nil
}

func (m *mockClientContext) Unsubscribe(target string) error {
	return nil
}

func (m *mockClientContext) Disconnect() error {
	return nil
}

func (m *mockClientContext) IsConnected() bool {
	return true
}

func (m *mockClientContext) ID() string {
	return m.clientID
}

func (m *mockClientContext) Connect() error {
	return nil
}

func (m *mockClientContext) StopReceiving() {
}

var _ client.Client = (*mockClientContext)(nil)

type mockMessage struct {
	topic    string
	payload  []byte
	qos      byte
	metadata map[string]any
}

func (m *mockMessage) Payload() []byte {
	return m.payload
}

func (m *mockMessage) Metadata() map[string]any {
	if m.metadata != nil {
		return m.metadata
	}
	return map[string]any{
		"topic": m.topic,
		"qos":   m.qos,
	}
}

var _ common.Message = (*mockMessage)(nil)

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
						Send: &config.SendActionConfig{
							Target:  "topic2",
							Payload: "hello",
							QoS:     1,
						},
					},
				},
			},
			clientID:    "client1",
			wantActions: 2,
			wantTypes:   []string{"SubscribeAction", "SendAction"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewDeclarativeBehavior(tt.config, logger)
			client := &mockClientContext{clientID: tt.clientID}
			actions := b.OnConnect(client)

			if len(actions) != tt.wantActions {
				t.Errorf("OnConnect() got %d actions, want %d", len(actions), tt.wantActions)
				return
			}

			for i, action := range actions {
				switch action.(type) {
				case *act.SubscribeAction:
					if tt.wantTypes[i] != "SubscribeAction" {
						t.Errorf("action[%d] = SubscribeAction, want %s", i, tt.wantTypes[i])
					}
				case *act.SendAction:
					if tt.wantTypes[i] != "SendAction" {
						t.Errorf("action[%d] = SendAction, want %s", i, tt.wantTypes[i])
					}
				case *act.UnsubscribeAction:
					if tt.wantTypes[i] != "UnsubscribeAction" {
						t.Errorf("action[%d] = UnsubscribeAction, want %s", i, tt.wantTypes[i])
					}
				case *act.DisconnectAction:
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
						Send: &config.SendActionConfig{
							Target:  "response/{{.MessageTopic}}",
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
						Send: &config.SendActionConfig{
							Target:  "devices/{{.ClientID}}/data",
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
			client := &mockClientContext{clientID: tt.clientID}
			msg := &mockMessage{topic: tt.msgTopic, payload: []byte(tt.msgPayload)}
			actions := b.OnMessage(client, msg)

			if len(actions) != tt.wantActions {
				t.Errorf("OnMessage() got %d actions, want %d", len(actions), tt.wantActions)
				return
			}

			if len(actions) > 0 {
				sendAction, ok := actions[0].(*act.SendAction)
				if ok {
					t.Logf("SendAction target: %s", sendAction.Target)
					t.Logf("SendAction payload: %s", sendAction.Payload)
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
						Send: &config.SendActionConfig{
							Target:  "heartbeat/{{.ClientID}}",
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
			client := &mockClientContext{clientID: tt.clientID}
			actions := b.OnTick(client, 0)

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
				Send: &config.SendActionConfig{
					Target:  "response/{{.MessageTopic}}",
					Payload: "from {{.ClientID}}: {{.MessagePayload}}",
					QoS:     0,
				},
			},
		},
	}, logger)

	client := &mockClientContext{clientID: "my-client"}
	msg := &mockMessage{topic: "test/topic", payload: []byte("hello")}

	actions := b.OnMessage(client, msg)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}

	sendAction, ok := actions[0].(*act.SendAction)
	if !ok {
		t.Fatalf("expected SendAction, got %T", actions[0])
	}

	expectedTarget := "response/test/topic"
	if sendAction.Target != expectedTarget {
		t.Errorf("target = %s, want %s", sendAction.Target, expectedTarget)
	}

	expectedPayload := "from my-client: hello"
	if sendAction.Payload != expectedPayload {
		t.Errorf("payload = %s, want %s", sendAction.Payload, expectedPayload)
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

	client := &mockClientContext{clientID: "client1"}
	actions := b.OnConnect(client)

	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}

	if _, ok := actions[0].(*act.UnsubscribeAction); !ok {
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

	client := &mockClientContext{clientID: "client1"}
	msg := &mockMessage{topic: "cmd/disconnect", payload: []byte("")}
	actions := b.OnMessage(client, msg)

	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}

	if _, ok := actions[0].(*act.DisconnectAction); !ok {
		t.Errorf("expected DisconnectAction, got %T", actions[0])
	}
}
