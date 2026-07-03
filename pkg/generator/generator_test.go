package generator

import (
	"testing"

	"mqtt-simulator/pkg/config"
)

func TestDefaultCredentialGenerator(t *testing.T) {
	tests := []struct {
		name         string
		cfg          config.Creds
		index        int
		wantClientID string
		wantUsername string
		wantPassword string
	}{
		{
			name: "with prefixes",
			cfg: config.Creds{
				ClientIDPrefix: "client-",
				UsernamePrefix: "user-",
				PasswordPrefix: "pass-",
			},
			index:        1,
			wantClientID: "client-1",
			wantUsername: "user-1",
			wantPassword: "pass-1",
		},
		{
			name: "with index 10",
			cfg: config.Creds{
				ClientIDPrefix: "client-",
				UsernamePrefix: "user-",
				PasswordPrefix: "pass-",
			},
			index:        10,
			wantClientID: "client-10",
			wantUsername: "user-10",
			wantPassword: "pass-10",
		},
		{
			name:         "empty prefixes",
			cfg:          config.Creds{},
			index:        5,
			wantClientID: "5",
			wantUsername: "5",
			wantPassword: "5",
		},
		{
			name: "with static client ID",
			cfg: config.Creds{
				ClientID: "static-client",
				Username: "static-user",
				Password: "static-pass",
			},
			index:        1,
			wantClientID: "static-client",
			wantUsername: "static-user",
			wantPassword: "static-pass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gen := NewDefaultCredentialGenerator(tt.cfg)

			if got := gen.GenerateClientID(tt.index); got != tt.wantClientID {
				t.Errorf("GenerateClientID(%d) = %v, want %v", tt.index, got, tt.wantClientID)
			}
			if got := gen.GenerateUsername(tt.index); got != tt.wantUsername {
				t.Errorf("GenerateUsername(%d) = %v, want %v", tt.index, got, tt.wantUsername)
			}
			if got := gen.GeneratePassword(tt.index); got != tt.wantPassword {
				t.Errorf("GeneratePassword(%d) = %v, want %v", tt.index, got, tt.wantPassword)
			}
		})
	}
}

func TestDefaultCredentialGenerator_EmptyPrefixes(t *testing.T) {
	cfg := config.Creds{}
	gen := NewDefaultCredentialGenerator(cfg)

	clientID := gen.GenerateClientID(5)
	if clientID == "" {
		t.Error("GenerateClientID() returned empty string")
	}

	username := gen.GenerateUsername(5)
	if username == "" {
		t.Error("GenerateUsername() returned empty string")
	}

	password := gen.GeneratePassword(5)
	if password == "" {
		t.Error("GeneratePassword() returned empty string")
	}
}
