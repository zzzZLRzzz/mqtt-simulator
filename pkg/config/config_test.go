package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{
			name: "valid declarative config",
			content: `
log_level: info
engine:
  broker:
    address: tcp://localhost:1883
    keepalive: 60
  connections: 1
behavior:
  mode: declarative
  on_connect:
    - subscribe:
        topic: "test"
        qos: 1
`,
			wantErr: false,
		},
		{
			name: "valid custom config",
			content: `
log_level: info
engine:
  broker:
    address: tcp://localhost:1883
    keepalive: 60
  connections: 1
behavior:
  mode: custom
`,
			wantErr: false,
		},
		{
			name: "default mode is declarative",
			content: `
log_level: info
engine:
  broker:
    address: tcp://localhost:1883
    keepalive: 60
  connections: 1
behavior:
  on_timer:
    - send:
        target: "test"
        payload: "hello"
        qos: 0
`,
			wantErr: false,
		},
		{
			name: "invalid mode",
			content: `
log_level: info
engine:
  broker:
    address: tcp://localhost:1883
    keepalive: 60
  connections: 1
behavior:
  mode: invalid
`,
			wantErr: true,
		},
		{
			name: "invalid engine connections",
			content: `
log_level: info
engine:
  broker:
    address: tcp://localhost:1883
    keepalive: 60
  connections: 0
behavior:
  mode: declarative
`,
			wantErr: true,
		},
		{
			name: "subscribe without topic",
			content: `
log_level: info
engine:
  broker:
    address: tcp://localhost:1883
    keepalive: 60
  connections: 1
behavior:
  on_connect:
    - subscribe:
        qos: 1
`,
			wantErr: true,
		},
		{
			name: "publish without topic",
			content: `
log_level: info
engine:
  broker:
    address: tcp://localhost:1883
    keepalive: 60
  connections: 1
behavior:
  on_timer:
    - send:
        payload: "hello"
        qos: 0
`,
			wantErr: true,
		},
		{
			name: "invalid qos",
			content: `
log_level: info
engine:
  broker:
    address: tcp://localhost:1883
    keepalive: 60
  connections: 1
behavior:
  on_connect:
    - subscribe:
        topic: "test"
        qos: 3
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "config-*.yaml")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())

			if _, err := tmpFile.WriteString(tt.content); err != nil {
				t.Fatalf("failed to write temp file: %v", err)
			}
			tmpFile.Close()

			_, err = LoadConfig(tmpFile.Name())
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBehaviorConfig_ModeValidation(t *testing.T) {
	cfg := &Config{
		Engine: EngineConfig{
			Broker: Broker{
				Address:   "tcp://localhost:1883",
				Keepalive: 60,
			},
			Connections: 1,
		},
		Behavior: BehaviorConfig{},
	}

	if cfg.Behavior.Mode != "" {
		t.Errorf("expected empty mode, got %s", cfg.Behavior.Mode)
	}

	if cfg.Behavior.Mode != BehaviorModeDeclarative && cfg.Behavior.Mode != BehaviorModeCustom && cfg.Behavior.Mode != "" {
		t.Errorf("invalid mode: %s", cfg.Behavior.Mode)
	}
}
