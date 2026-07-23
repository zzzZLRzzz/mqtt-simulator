package xmpp

import (
	"testing"
)

func TestMessageStanzaMetadata_ToMap(t *testing.T) {
	meta := MessageStanzaMetadata{
		XMPPBaseMetadata: XMPPBaseMetadata{
			Kind: StanzaKindMessage,
			From: "sender@example.com",
			To:   "receiver@example.com",
			ID:   "msg-123",
		},
		Type:    "chat",
		Subject: "Test Subject",
		Thread:  "thread-456",
		Lang:    "en",
	}

	result := meta.ToMap()

	expected := map[string]any{
		MetadataKeyStanzaKind: StanzaKindMessage,
		MetadataKeyFrom:       "sender@example.com",
		MetadataKeyTo:         "receiver@example.com",
		MetadataKeyID:         "msg-123",
		MetadataKeyType:       "chat",
		MetadataKeySubject:    "Test Subject",
		MetadataKeyThread:     "thread-456",
		MetadataKeyLang:       "en",
	}

	for k, v := range expected {
		if result[k] != v {
			t.Errorf("ToMap() key %s = %v, want %v", k, result[k], v)
		}
	}
}

func TestIQStanzaMetadata_ToMap(t *testing.T) {
	meta := IQStanzaMetadata{
		XMPPBaseMetadata: XMPPBaseMetadata{
			Kind: StanzaKindIQ,
			From: "client@example.com",
			To:   "server@example.com",
			ID:   "iq-789",
		},
		Type: "get",
	}

	result := meta.ToMap()

	expected := map[string]any{
		MetadataKeyStanzaKind: StanzaKindIQ,
		MetadataKeyFrom:       "client@example.com",
		MetadataKeyTo:         "server@example.com",
		MetadataKeyID:         "iq-789",
		MetadataKeyType:       "get",
	}

	for k, v := range expected {
		if result[k] != v {
			t.Errorf("ToMap() key %s = %v, want %v", k, result[k], v)
		}
	}
}

func TestPresenceStanzaMetadata_ToMap(t *testing.T) {
	meta := PresenceStanzaMetadata{
		XMPPBaseMetadata: XMPPBaseMetadata{
			Kind: StanzaKindPresence,
			From: "user@example.com",
			To:   "contact@example.com",
			ID:   "pres-001",
		},
		Type:     "available",
		Show:     "chat",
		Status:   "Online",
		Priority: 10,
	}

	result := meta.ToMap()

	expected := map[string]any{
		MetadataKeyStanzaKind: StanzaKindPresence,
		MetadataKeyFrom:       "user@example.com",
		MetadataKeyTo:         "contact@example.com",
		MetadataKeyID:         "pres-001",
		MetadataKeyType:       "available",
		MetadataKeyShow:       "chat",
		MetadataKeyStatus:     "Online",
		MetadataKeyPriority:   int8(10),
	}

	for k, v := range expected {
		if result[k] != v {
			t.Errorf("ToMap() key %s = %v, want %v", k, result[k], v)
		}
	}
}

func TestParseMessageMetadata_RoundTrip(t *testing.T) {
	original := MessageStanzaMetadata{
		XMPPBaseMetadata: XMPPBaseMetadata{
			Kind: StanzaKindMessage,
			From: "sender@example.com",
			To:   "receiver@example.com",
			ID:   "msg-roundtrip",
		},
		Type:    "groupchat",
		Subject: "RoundTrip Test",
		Thread:  "thread-rt",
		Lang:    "zh",
	}

	m := original.ToMap()
	parsed, err := ParseStanzaMetadata("", m)
	if err != nil {
		t.Fatalf("ParseStanzaMetadata() error = %v", err)
	}

	msgMeta, ok := parsed.(*MessageStanzaMetadata)
	if !ok {
		t.Fatalf("ParseStanzaMetadata() returned wrong type: %T", parsed)
	}

	if msgMeta.Kind != original.Kind {
		t.Errorf("RoundTrip Kind = %v, want %v", msgMeta.Kind, original.Kind)
	}
	if msgMeta.From != original.From {
		t.Errorf("RoundTrip From = %v, want %v", msgMeta.From, original.From)
	}
	if msgMeta.To != original.To {
		t.Errorf("RoundTrip To = %v, want %v", msgMeta.To, original.To)
	}
	if msgMeta.ID != original.ID {
		t.Errorf("RoundTrip ID = %v, want %v", msgMeta.ID, original.ID)
	}
	if string(msgMeta.Type) != string(original.Type) {
		t.Errorf("RoundTrip Type = %v, want %v", msgMeta.Type, original.Type)
	}
	if msgMeta.Subject != original.Subject {
		t.Errorf("RoundTrip Subject = %v, want %v", msgMeta.Subject, original.Subject)
	}
	if msgMeta.Thread != original.Thread {
		t.Errorf("RoundTrip Thread = %v, want %v", msgMeta.Thread, original.Thread)
	}
	if msgMeta.Lang != original.Lang {
		t.Errorf("RoundTrip Lang = %v, want %v", msgMeta.Lang, original.Lang)
	}
}

func TestParseIQMetadata_RoundTrip(t *testing.T) {
	original := IQStanzaMetadata{
		XMPPBaseMetadata: XMPPBaseMetadata{
			Kind: StanzaKindIQ,
			From: "client@example.com",
			To:   "server@example.com",
			ID:   "iq-roundtrip",
		},
		Type: "set",
	}

	m := original.ToMap()
	parsed, err := ParseStanzaMetadata("", m)
	if err != nil {
		t.Fatalf("ParseStanzaMetadata() error = %v", err)
	}

	iqMeta, ok := parsed.(*IQStanzaMetadata)
	if !ok {
		t.Fatalf("ParseStanzaMetadata() returned wrong type: %T", parsed)
	}

	if iqMeta.Kind != original.Kind {
		t.Errorf("RoundTrip Kind = %v, want %v", iqMeta.Kind, original.Kind)
	}
	if iqMeta.From != original.From {
		t.Errorf("RoundTrip From = %v, want %v", iqMeta.From, original.From)
	}
	if iqMeta.To != original.To {
		t.Errorf("RoundTrip To = %v, want %v", iqMeta.To, original.To)
	}
	if iqMeta.ID != original.ID {
		t.Errorf("RoundTrip ID = %v, want %v", iqMeta.ID, original.ID)
	}
	if string(iqMeta.Type) != string(original.Type) {
		t.Errorf("RoundTrip Type = %v, want %v", iqMeta.Type, original.Type)
	}
}

func TestParsePresenceMetadata_RoundTrip(t *testing.T) {
	original := PresenceStanzaMetadata{
		XMPPBaseMetadata: XMPPBaseMetadata{
			Kind: StanzaKindPresence,
			From: "user@example.com",
			To:   "contact@example.com",
			ID:   "pres-roundtrip",
		},
		Type:     "subscribe",
		Show:     "away",
		Status:   "Busy",
		Priority: -5,
	}

	m := original.ToMap()
	parsed, err := ParseStanzaMetadata("", m)
	if err != nil {
		t.Fatalf("ParseStanzaMetadata() error = %v", err)
	}

	presMeta, ok := parsed.(*PresenceStanzaMetadata)
	if !ok {
		t.Fatalf("ParseStanzaMetadata() returned wrong type: %T", parsed)
	}

	if presMeta.Kind != original.Kind {
		t.Errorf("RoundTrip Kind = %v, want %v", presMeta.Kind, original.Kind)
	}
	if presMeta.From != original.From {
		t.Errorf("RoundTrip From = %v, want %v", presMeta.From, original.From)
	}
	if presMeta.To != original.To {
		t.Errorf("RoundTrip To = %v, want %v", presMeta.To, original.To)
	}
	if presMeta.ID != original.ID {
		t.Errorf("RoundTrip ID = %v, want %v", presMeta.ID, original.ID)
	}
	if string(presMeta.Type) != string(original.Type) {
		t.Errorf("RoundTrip Type = %v, want %v", presMeta.Type, original.Type)
	}
	if presMeta.Show != original.Show {
		t.Errorf("RoundTrip Show = %v, want %v", presMeta.Show, original.Show)
	}
	if presMeta.Status != original.Status {
		t.Errorf("RoundTrip Status = %v, want %v", presMeta.Status, original.Status)
	}
	if presMeta.Priority != original.Priority {
		t.Errorf("RoundTrip Priority = %v, want %v", presMeta.Priority, original.Priority)
	}
}

func TestParseBaseMetadata_ToPriority(t *testing.T) {
	testCases := []struct {
		name    string
		input   map[string]any
		want    int8
		wantErr bool
	}{
		{"int8 value", map[string]any{"kind": "presence", "priority": int8(10)}, 10, false},
		{"int value", map[string]any{"kind": "presence", "priority": 10}, 10, false},
		{"float64 value", map[string]any{"kind": "presence", "priority": float64(10)}, 10, false},
		{"string value", map[string]any{"kind": "presence", "priority": "10"}, 10, false},
		{"negative value", map[string]any{"kind": "presence", "priority": -5}, -5, false},
		{"max value", map[string]any{"kind": "presence", "priority": 127}, 127, false},
		{"min value", map[string]any{"kind": "presence", "priority": -128}, -128, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseStanzaMetadata("", tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("ParseStanzaMetadata() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if !tc.wantErr {
				presMeta := parsed.(*PresenceStanzaMetadata)
				if presMeta.Priority != tc.want {
					t.Errorf("Priority = %v, want %v", presMeta.Priority, tc.want)
				}
			}
		})
	}
}
