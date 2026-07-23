package xmpp

import (
	"fmt"
	"strconv"

	"gosrc.io/xmpp/stanza"
)

const (
	MetadataKeyStanzaKind = "kind"
	MetadataKeyFrom       = "from"
	MetadataKeyTo         = "to"
	MetadataKeyType       = "type"
	MetadataKeyID         = "id"

	// Message optional
	MetadataKeySubject = "subject"
	MetadataKeyThread  = "thread"
	MetadataKeyLang    = "lang"

	// Presence optional
	MetadataKeyShow     = "show"
	MetadataKeyStatus   = "status"
	MetadataKeyPriority = "priority"
)

const (
	StanzaKindMessage  = "message"
	StanzaKindIQ       = "iq"
	StanzaKindPresence = "presence"
)

type MessageType = stanza.StanzaType
type IQType = stanza.StanzaType
type PresenceType = stanza.StanzaType

type StanzaMetadata interface {
	GetKind() string
	GetFrom() string
	GetTo() string
	GetID() string
}

type XMPPBaseMetadata struct {
	Kind string // required, identifies stanza kind: "message", "iq", "presence"
	From string // optional, usually filled by server
	To   string // required for Message/IQ, optional for Presence (empty = broadcast)
	ID   string // REQUIRED for IQ, optional for Message/Presence
}

func (m XMPPBaseMetadata) GetKind() string { return m.Kind }
func (m XMPPBaseMetadata) GetFrom() string { return m.From }
func (m XMPPBaseMetadata) GetTo() string   { return m.To }
func (m XMPPBaseMetadata) GetID() string   { return m.ID }

type MessageStanzaMetadata struct {
	XMPPBaseMetadata
	Type    MessageType // required, e.g., chat, groupchat, normal
	Subject string      // optional, message subject/title
	Thread  string      // optional, thread identifier for grouping messages
	Lang    string      // optional, xml:lang for language identification
}

type IQStanzaMetadata struct {
	XMPPBaseMetadata
	Type IQType // required, e.g., get, set, result, error
}

type PresenceStanzaMetadata struct {
	XMPPBaseMetadata
	Type     PresenceType // required, e.g., subscribe, unavailable, empty=available
	Show     string       // optional, available values: away, chat, dnd, xa
	Status   string       // optional, user-defined status text
	Priority int8         // optional, range: -128 to 127
}

// ParseStanzaMetadata parses metadata map into corresponding StanzaMetadata
func ParseStanzaMetadata(target string, metadata map[string]any) (StanzaMetadata, error) {
	kind, ok := metadata[MetadataKeyStanzaKind].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid 'kind' field")
	}

	switch kind {
	case StanzaKindMessage:
		return parseMessageMetadata(target, metadata)
	case StanzaKindIQ:
		return parseIQMetadata(target, metadata)
	case StanzaKindPresence:
		return parsePresenceMetadata(target, metadata)
	default:
		return nil, fmt.Errorf("unknown stanza kind: %s", kind)
	}
}

func parseBaseMetadata(target string, metadata map[string]any) (XMPPBaseMetadata, error) {
	var result XMPPBaseMetadata

	kind, ok := metadata[MetadataKeyStanzaKind].(string)
	if !ok {
		return result, fmt.Errorf("missing or invalid 'kind' field")
	}
	result.Kind = kind

	if from, ok := metadata[MetadataKeyFrom].(string); ok {
		result.From = from
	}

	if to, ok := metadata[MetadataKeyTo].(string); ok && to != "" {
		result.To = to
	} else {
		result.To = target
	}

	if id, ok := metadata[MetadataKeyID].(string); ok {
		result.ID = id
	}

	return result, nil
}

func parseMessageMetadata(target string, metadata map[string]any) (StanzaMetadata, error) {
	base, err := parseBaseMetadata(target, metadata)
	if err != nil {
		return nil, err
	}

	result := &MessageStanzaMetadata{
		XMPPBaseMetadata: base,
	}

	// parse required field
	if t, ok := metadata[MetadataKeyType].(string); ok {
		result.Type = MessageType(t)
	} else {
		return nil, fmt.Errorf("missing 'type' field for message stanza")
	}

	// parse optional field
	if subject, ok := metadata[MetadataKeySubject].(string); ok {
		result.Subject = subject
	}
	if thread, ok := metadata[MetadataKeyThread].(string); ok {
		result.Thread = thread
	}
	if lang, ok := metadata[MetadataKeyLang].(string); ok {
		result.Lang = lang
	}

	return result, nil
}

func parseIQMetadata(target string, metadata map[string]any) (StanzaMetadata, error) {
	base, err := parseBaseMetadata(target, metadata)
	if err != nil {
		return nil, err
	}

	result := &IQStanzaMetadata{
		XMPPBaseMetadata: base,
	}

	// parse required field
	if t, ok := metadata[MetadataKeyType].(string); ok {
		result.Type = IQType(t)
	} else {
		return nil, fmt.Errorf("missing 'type' field for IQ stanza")
	}

	if result.ID == "" {
		return nil, fmt.Errorf("ID is required for IQ stanza")
	}

	return result, nil
}

func parsePresenceMetadata(target string, metadata map[string]any) (StanzaMetadata, error) {
	base, err := parseBaseMetadata(target, metadata)
	if err != nil {
		return nil, err
	}

	result := &PresenceStanzaMetadata{
		XMPPBaseMetadata: base,
	}

	// parse optional field
	if t, ok := metadata[MetadataKeyType].(string); ok {
		result.Type = PresenceType(t)
	}

	// parse optional field
	if show, ok := metadata[MetadataKeyShow].(string); ok {
		result.Show = show
	}
	if status, ok := metadata[MetadataKeyStatus].(string); ok {
		result.Status = status
	}
	if priority, ok := metadata[MetadataKeyPriority].(int8); ok {
		result.Priority = priority
	} else if priority, ok := metadata[MetadataKeyPriority].(int); ok {
		// Handle int parsed from JSON
		if priority >= -128 && priority <= 127 {
			result.Priority = int8(priority)
		} else {
			return nil, fmt.Errorf("priority out of range: %d (must be -128 to 127)", priority)
		}
	} else if priority, ok := metadata[MetadataKeyPriority].(float64); ok {
		// Handle float64 parsed from JSON
		if priority >= -128 && priority <= 127 {
			result.Priority = int8(priority)
		} else {
			return nil, fmt.Errorf("priority out of range: %f (must be -128 to 127)", priority)
		}
	} else if priorityStr, ok := metadata[MetadataKeyPriority].(string); ok {
		// Handle numeric string
		if p, err := strconv.Atoi(priorityStr); err == nil {
			if p >= -128 && p <= 127 {
				result.Priority = int8(p)
			} else {
				return nil, fmt.Errorf("priority out of range: %d (must be -128 to 127)", p)
			}
		} else {
			return nil, fmt.Errorf("invalid priority format: %s", priorityStr)
		}
	}

	return result, nil
}

func (m MessageStanzaMetadata) GetType() stanza.StanzaType {
	return stanza.StanzaType(m.Type)
}

func (m IQStanzaMetadata) GetType() stanza.StanzaType {
	return stanza.StanzaType(m.Type)
}

func (m PresenceStanzaMetadata) GetType() stanza.StanzaType {
	return stanza.StanzaType(m.Type)
}

func (m XMPPBaseMetadata) ToMap() map[string]any {
	return map[string]any{
		MetadataKeyStanzaKind: m.Kind,
		MetadataKeyFrom:       m.From,
		MetadataKeyTo:         m.To,
		MetadataKeyID:         m.ID,
	}
}

func (m MessageStanzaMetadata) ToMap() map[string]any {
	result := m.XMPPBaseMetadata.ToMap()
	result[MetadataKeyType] = string(m.Type)
	result[MetadataKeySubject] = m.Subject
	result[MetadataKeyThread] = m.Thread
	result[MetadataKeyLang] = m.Lang
	return result
}

func (m IQStanzaMetadata) ToMap() map[string]any {
	result := m.XMPPBaseMetadata.ToMap()
	result[MetadataKeyType] = string(m.Type)
	return result
}

func (m PresenceStanzaMetadata) ToMap() map[string]any {
	result := m.XMPPBaseMetadata.ToMap()
	result[MetadataKeyType] = string(m.Type)
	result[MetadataKeyShow] = m.Show
	result[MetadataKeyStatus] = m.Status
	result[MetadataKeyPriority] = m.Priority
	return result
}
