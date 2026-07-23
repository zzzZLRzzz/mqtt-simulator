package behavior

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	act "conn-conductor/pkg/action"
	xmppmeta "conn-conductor/pkg/action/xmpp"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/cwmp"
	"conn-conductor/pkg/logging"
)

func TestCWMPBehavior_OnMessage_ConnectionRequest_Success(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("<soap:Envelope/>"))
	}))
	defer testServer.Close()

	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := NewCWMPBehavior(config.BehaviorConfig{
		Custom: map[string]any{
			"acs_url":                     testServer.URL,
			"connection_request_username": "admin",
			"connection_request_password": "password",
			"manufacturer":                "TestVendor",
			"oui":                         "001122",
			"product_class":               "TestProduct",
			"serial_number":               "SN12345",
		},
	}, logger)

	cr := &cwmp.ConnectionRequest{
		Username: "admin",
		Password: "password",
	}
	payloadXML, _ := xml.Marshal(cr)

	msg := &mockMessage{
		payload: string(payloadXML),
		metadata: map[string]any{
			xmppmeta.MetadataKeyStanzaKind: xmppmeta.StanzaKindIQ,
			xmppmeta.MetadataKeyFrom:       "acs@xmpp.example.com",
			xmppmeta.MetadataKeyID:         "cr1",
			xmppmeta.MetadataKeyType:       "get",
		},
	}

	client := &mockClientContext{clientID: "cpe123"}
	actions := b.OnMessage(client, msg)

	if len(actions) != 1 {
		t.Fatalf("expected 1 action (response), got %d", len(actions))
	}

	responseAction, ok := actions[0].(*act.SendAction)
	if !ok {
		t.Fatalf("expected SendAction, got %T", actions[0])
	}

	metaType, ok := responseAction.Metadata[xmppmeta.MetadataKeyType].(string)
	if !ok || metaType != "result" {
		t.Errorf("response IQ type = %s, want result", metaType)
	}

	metaID, ok := responseAction.Metadata[xmppmeta.MetadataKeyID].(string)
	if !ok || metaID != "cr1" {
		t.Errorf("response IQ ID = %s, want cr1", metaID)
	}

	metaTo, ok := responseAction.Metadata[xmppmeta.MetadataKeyTo].(string)
	if !ok || metaTo != "acs@xmpp.example.com" {
		t.Errorf("response IQ To = %s, want acs@xmpp.example.com", metaTo)
	}
}

func TestCWMPBehavior_OnMessage_ConnectionRequest_Failure(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := &CWMPBehavior{
		logger: logger,
		informConfig: cwmp.InformConfig{
			Manufacturer: "TestVendor",
			OUI:          "001122",
			ProductClass: "TestProduct",
			SerialNumber: "SN12345",
		},
		connectionRequestUsername: "admin",
		connectionRequestPassword: "correctpass",
		httpClient:                &http.Client{},
	}

	cr := &cwmp.ConnectionRequest{
		Username: "admin",
		Password: "wrongpass",
	}
	payloadXML, _ := xml.Marshal(cr)

	msg := &mockMessage{
		payload: string(payloadXML),
		metadata: map[string]any{
			xmppmeta.MetadataKeyStanzaKind: xmppmeta.StanzaKindIQ,
			xmppmeta.MetadataKeyFrom:       "acs@xmpp.example.com",
			xmppmeta.MetadataKeyID:         "cr1",
			xmppmeta.MetadataKeyType:       "get",
		},
	}

	client := &mockClientContext{clientID: "cpe123"}
	actions := b.OnMessage(client, msg)

	if len(actions) != 1 {
		t.Fatalf("expected 1 action (error response), got %d", len(actions))
	}

	responseAction, ok := actions[0].(*act.SendAction)
	if !ok {
		t.Fatalf("expected SendAction, got %T", actions[0])
	}

	metaType, ok := responseAction.Metadata[xmppmeta.MetadataKeyType].(string)
	if !ok || metaType != "error" {
		t.Errorf("response IQ type = %s, want error", metaType)
	}

	metaID, ok := responseAction.Metadata[xmppmeta.MetadataKeyID].(string)
	if !ok || metaID != "cr1" {
		t.Errorf("response IQ ID = %s, want cr1", metaID)
	}
}

func TestCWMPBehavior_OnMessage_NonConnectionRequest(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := NewCWMPBehavior(config.BehaviorConfig{}, logger)

	msg := &mockMessage{
		payload: "<query xmlns=\"jabber:iq:roster\"/>",
		metadata: map[string]any{
			xmppmeta.MetadataKeyStanzaKind: xmppmeta.StanzaKindIQ,
			xmppmeta.MetadataKeyFrom:       "user@xmpp.example.com",
			xmppmeta.MetadataKeyID:         "iq1",
			xmppmeta.MetadataKeyType:       "get",
		},
	}

	client := &mockClientContext{clientID: "cpe123"}
	actions := b.OnMessage(client, msg)

	if len(actions) != 0 {
		t.Errorf("expected 0 actions for non-ConnectionRequest IQ, got %d", len(actions))
	}
}

func TestCWMPBehavior_OnMessage_MessageStanza(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := NewCWMPBehavior(config.BehaviorConfig{}, logger)

	msg := &mockMessage{
		payload: "hello",
		metadata: map[string]any{
			xmppmeta.MetadataKeyStanzaKind: xmppmeta.StanzaKindMessage,
			xmppmeta.MetadataKeyFrom:       "user@xmpp.example.com",
		},
	}

	client := &mockClientContext{clientID: "cpe123"}
	actions := b.OnMessage(client, msg)

	if len(actions) != 0 {
		t.Errorf("expected 0 actions for Message stanza, got %d", len(actions))
	}
}

func TestCWMPBehavior_OnMessage_PresenceStanza(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := NewCWMPBehavior(config.BehaviorConfig{}, logger)

	msg := &mockMessage{
		payload: "{\"show\":\"chat\",\"status\":\"online\",\"priority\":5}",
		metadata: map[string]any{
			xmppmeta.MetadataKeyStanzaKind: xmppmeta.StanzaKindPresence,
			xmppmeta.MetadataKeyFrom:       "user@xmpp.example.com",
		},
	}

	client := &mockClientContext{clientID: "cpe123"}
	actions := b.OnMessage(client, msg)

	if len(actions) != 0 {
		t.Errorf("expected 0 actions for Presence stanza, got %d", len(actions))
	}
}

func TestCWMPBehavior_OnMessage_IQSet(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := NewCWMPBehavior(config.BehaviorConfig{}, logger)

	cr := &cwmp.ConnectionRequest{
		Username: "admin",
		Password: "password",
	}
	payloadXML, _ := xml.Marshal(cr)

	msg := &mockMessage{
		payload: string(payloadXML),
		metadata: map[string]any{
			xmppmeta.MetadataKeyStanzaKind: xmppmeta.StanzaKindIQ,
			xmppmeta.MetadataKeyFrom:       "acs@xmpp.example.com",
			xmppmeta.MetadataKeyID:         "cr1",
			xmppmeta.MetadataKeyType:       "set",
		},
	}

	client := &mockClientContext{clientID: "cpe123"}
	actions := b.OnMessage(client, msg)

	if len(actions) != 0 {
		t.Errorf("expected 0 actions for IQ set type, got %d", len(actions))
	}
}

func TestCWMPBehavior_SendInformHTTP_NoACSURL(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := &CWMPBehavior{
		logger:     logger,
		httpClient: &http.Client{},
	}

	b.sendInformHTTP("cpe123", cwmp.InformConfig{})
}

func TestCWMPBehavior_OnConnect_NoAction(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := NewCWMPBehavior(config.BehaviorConfig{}, logger)

	client := &mockClientContext{clientID: "cpe123"}
	actions := b.OnConnect(client)

	if len(actions) != 0 {
		t.Errorf("expected 0 actions on connect, got %d", len(actions))
	}
}

func TestCWMPBehavior_OnTick_NoAction(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := NewCWMPBehavior(config.BehaviorConfig{}, logger)

	client := &mockClientContext{clientID: "cpe123"}
	actions := b.OnTick(client, 10)

	if len(actions) != 0 {
		t.Errorf("expected 0 actions on tick, got %d", len(actions))
	}
}

func TestCWMPBehavior_InformInterval_Default(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := NewCWMPBehavior(config.BehaviorConfig{}, logger).(*CWMPBehavior)

	if b.informInterval != 300 {
		t.Errorf("default informInterval = %d, want 300", b.informInterval)
	}
}

func TestCWMPBehavior_InformInterval_FromConfig(t *testing.T) {
	logger := logging.NewLogger(logging.LogLevelError, "test")
	b := NewCWMPBehavior(config.BehaviorConfig{
		Custom: map[string]any{
			"inform_interval": int64(10),
		},
	}, logger).(*CWMPBehavior)

	if b.informInterval != 10 {
		t.Errorf("informInterval = %d, want 10", b.informInterval)
	}
}
