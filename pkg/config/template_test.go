package config

import (
	"testing"
)

func TestExecuteTemplate(t *testing.T) {
	tests := []struct {
		name        string
		tmpl        string
		data        TemplateData
		wantContains string
		wantErr     bool
	}{
		{
			name:        "empty template",
			tmpl:        "",
			data:        TemplateData{},
			wantContains: "",
			wantErr:     false,
		},
		{
			name:        "client id template",
			tmpl:        "client-{{.ClientID}}",
			data:        TemplateData{ClientID: "test-client"},
			wantContains: "client-test-client",
			wantErr:     false,
		},
		{
			name:        "message topic template",
			tmpl:        "response/{{.MessageTopic}}",
			data:        TemplateData{MessageTopic: "devices/test"},
			wantContains: "response/devices/test",
			wantErr:     false,
		},
		{
			name:        "message payload template",
			tmpl:        "echo: {{.MessagePayload}}",
			data:        TemplateData{MessagePayload: "hello"},
			wantContains: "echo: hello",
			wantErr:     false,
		},
		{
			name:        "combined template",
			tmpl:        "{{.ClientID}}/{{.MessageTopic}}",
			data:        TemplateData{ClientID: "client1", MessageTopic: "topic1"},
			wantContains: "client1/topic1",
			wantErr:     false,
		},
		{
			name:        "random int",
			tmpl:        "{{RandomInt 1 100}}",
			data:        TemplateData{},
			wantContains: "",
			wantErr:     false,
		},
		{
			name:        "random float",
			tmpl:        "{{RandomFloat 10 20}}",
			data:        TemplateData{},
			wantContains: "",
			wantErr:     false,
		},
		{
			name:        "now unix",
			tmpl:        "{{NowUnix}}",
			data:        TemplateData{},
			wantContains: "",
			wantErr:     false,
		},
		{
			name:        "invalid template",
			tmpl:        "{{.InvalidFunc}}",
			data:        TemplateData{},
			wantContains: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecuteTemplate(tt.tmpl, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteTemplate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if tt.wantContains != "" && got != tt.wantContains {
					t.Errorf("ExecuteTemplate() = %v, want %v", got, tt.wantContains)
				}
			}
		})
	}
}
