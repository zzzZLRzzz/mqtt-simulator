package xmpp

import (
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strings"
	"sync"
	"time"

	xmpplib "gosrc.io/xmpp"
	"gosrc.io/xmpp/stanza"

	act "conn-conductor/pkg/action"
	"conn-conductor/pkg/action/xmpp"
	xmppmeta "conn-conductor/pkg/action/xmpp"
	"conn-conductor/pkg/behavior"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/common"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/logging"
	"conn-conductor/pkg/metrics"
)

type StanzaType string

const (
	StanzaTypeMessage  StanzaType = "message"
	StanzaTypeIQ       StanzaType = "iq"
	StanzaTypePresence StanzaType = "presence"
)

type Client struct {
	jID            string
	client         *xmpplib.Client
	streamManager  *xmpplib.StreamManager
	broker         config.Broker
	logger         *logging.Logger
	creds          config.Creds
	behavior       behavior.Behavior
	actionHandler  act.ActionHandler
	mu             sync.RWMutex
	isShuttingDown bool
	isReceiving    bool
	isConnected    bool
	iqResponses    sync.Map
	iqIDCounter    int
}

type iqResponse struct {
	iq       *stanza.IQ
	received bool
	err      error
}

type messageAdapter struct {
	msg *stanza.Message
}

func (m *messageAdapter) Payload() string {
	return m.msg.Body
}

func (m *messageAdapter) Metadata() map[string]any {
	return (&xmppmeta.MessageStanzaMetadata{
		XMPPBaseMetadata: xmppmeta.XMPPBaseMetadata{
			Kind: xmppmeta.StanzaKindMessage,
			From: m.msg.From,
			To:   m.msg.To,
			ID:   m.msg.Id,
		},
		Type:    xmppmeta.MessageType(m.msg.Type),
		Subject: m.msg.Subject,
		Thread:  m.msg.Thread,
		Lang:    m.msg.Lang,
	}).ToMap()
}

var _ common.Message = (*messageAdapter)(nil)

type iqAdapter struct {
	iq *stanza.IQ
}

func (i *iqAdapter) Payload() string {
	if i.iq.Payload == nil {
		return ""
	}
	data, err := xml.Marshal(i.iq.Payload)
	if err != nil {
		return ""
	}
	return string(data)
}

func (i *iqAdapter) Metadata() map[string]any {
	return (&xmppmeta.IQStanzaMetadata{
		XMPPBaseMetadata: xmppmeta.XMPPBaseMetadata{
			Kind: xmppmeta.StanzaKindIQ,
			From: i.iq.From,
			To:   i.iq.To,
			ID:   i.iq.Id,
		},
		Type: xmppmeta.IQType(i.iq.Type),
	}).ToMap()
}

var _ common.Message = (*iqAdapter)(nil)

type presenceAdapter struct {
	presence *stanza.Presence
}

func (p *presenceAdapter) Payload() string {
	payload := map[string]any{
		"show":     string(p.presence.Show),
		"status":   p.presence.Status,
		"priority": p.presence.Priority,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return string(data)
}

func (p *presenceAdapter) Metadata() map[string]any {
	return (&xmppmeta.PresenceStanzaMetadata{
		XMPPBaseMetadata: xmppmeta.XMPPBaseMetadata{
			Kind: xmppmeta.StanzaKindPresence,
			From: p.presence.From,
			To:   p.presence.To,
			ID:   p.presence.Id,
		},
		Type:     xmppmeta.PresenceType(p.presence.Type),
		Show:     string(p.presence.Show),
		Status:   p.presence.Status,
		Priority: p.presence.Priority,
	}).ToMap()
}

var _ common.Message = (*presenceAdapter)(nil)

func NewClient(
	logger *logging.Logger,
	broker config.Broker,
	index int,
	creds config.Creds,
	beh behavior.Behavior,
	actionHandler act.ActionHandler,
) *Client {
	if beh == nil {
		panic("behavior cannot be nil")
	}
	if actionHandler == nil {
		panic("actionHandler cannot be nil")
	}

	m := &Client{
		jID:           creds.ClientID,
		broker:        broker,
		logger:        logger,
		creds:         creds,
		behavior:      beh,
		actionHandler: actionHandler,
		isReceiving:   true,
		iqIDCounter:   0,
	}

	return m
}

func (m *Client) Connect() error {
	config := xmpplib.Config{
		TransportConfiguration: xmpplib.TransportConfiguration{
			Address: m.broker.Address,
		},
		Jid:            m.creds.ClientID,
		Credential:     xmpplib.Password(m.creds.Password),
		ConnectTimeout: int(m.broker.Timeout.Seconds()),
		Insecure:       !m.broker.TLS,
	}

	if m.broker.TLS {
		tlsConfig := &tls.Config{}
		if m.broker.CAFile == "" && m.broker.CertFile == "" && m.broker.KeyFile == "" {
			tlsConfig = &tls.Config{InsecureSkipVerify: true}
		}
		config.TLSConfig = tlsConfig
	}

	router := xmpplib.NewRouter()
	router.HandleFunc(string(StanzaTypeMessage), m.handlePacket)
	router.HandleFunc(string(StanzaTypeIQ), m.handlePacket)
	router.HandleFunc(string(StanzaTypePresence), m.handlePacket)

	client, err := xmpplib.NewClient(&config, router, m.errorHandler)
	if err != nil {
		return fmt.Errorf("failed to create XMPP client: %w", err)
	}

	m.client = client

	postConnect := func(c xmpplib.Sender) {
		m.mu.Lock()
		m.isConnected = true
		m.mu.Unlock()

		m.logger.Info("[%s] connected to %s", m.jID, m.broker.Address)

		actions := m.behavior.OnConnect(m)
		m.actionHandler(m, actions)
	}

	m.streamManager = xmpplib.NewStreamManager(client, postConnect)

	go func() {
		if err := m.streamManager.Run(); err != nil && !m.isShuttingDown {
			m.logger.Error("[%s] stream manager error: %v", m.jID, err)
			m.mu.Lock()
			m.isConnected = false
			m.mu.Unlock()
		}
	}()

	return nil
}

func (m *Client) handlePacket(s xmpplib.Sender, p stanza.Packet) {
	m.mu.RLock()
	isReceiving := m.isReceiving
	m.mu.RUnlock()
	if !isReceiving {
		return
	}

	var msg common.Message
	var from string

	switch v := p.(type) {
	case *stanza.Message:
		msg = &messageAdapter{msg: v}
		from = v.From
	case *stanza.IQ:
		if v.Type == stanza.IQTypeResult || v.Type == stanza.IQTypeError {
			if val, ok := m.iqResponses.LoadAndDelete(v.Id); ok {
				resp, _ := val.(*iqResponse)
				resp.iq = v
				resp.received = true
				if v.Type == stanza.IQTypeError {
					resp.err = fmt.Errorf("IQ error: %v", v.Error)
				}
			}
			return
		}
		msg = &iqAdapter{iq: v}
		from = v.From
	case *stanza.Presence:
		msg = &presenceAdapter{presence: v}
		from = v.From
	default:
		return
	}

	metrics.MessagesReceived.WithLabelValues(from).Inc()
	metrics.IncReceived()

	actions := m.behavior.OnMessage(m, msg)
	m.actionHandler(m, actions)
}

func (m *Client) errorHandler(err error) {
	errStr := err.Error()
	if strings.Contains(errStr, "unknown namespace") ||
		strings.Contains(errStr, "not-well-formed") ||
		strings.Contains(errStr, "NextStart") ||
		strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "connection closed") {
		m.logger.Debug("[%s] stream error (non-critical): %v", m.jID, err)
		return
	}
	m.logger.Warn("[%s] error: %v", m.jID, err)
}

func (m *Client) StopReceiving() {
	m.mu.Lock()
	m.isReceiving = false
	m.mu.Unlock()
}

func (m *Client) generateStanzaID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.iqIDCounter++
	return fmt.Sprintf("%s-%d", m.jID, m.iqIDCounter)
}

func (m *Client) Subscribe(target string, metadata map[string]any) error {
	return nil
}

func (m *Client) Unsubscribe(target string) error {
	return nil
}

func (m *Client) Send(payload any, target string, metadata map[string]any) error {
	stanzaMetaData, err := xmppmeta.ParseStanzaMetadata(target, metadata)
	if err != nil {
		return err
	}

	switch v := stanzaMetaData.(type) {
	case *xmppmeta.MessageStanzaMetadata:
		return m.sendMessage(payload, target, v)
	case *xmppmeta.IQStanzaMetadata:
		return m.sendIQ(payload, target, v)
	case *xmppmeta.PresenceStanzaMetadata:
		return m.sendPresence(payload, target, v)
	default:
		return fmt.Errorf("unsupported stanza kind: %s", stanzaMetaData.GetKind())
	}
}

func (m *Client) sendMessage(payload any, target string, metadata *xmpp.MessageStanzaMetadata) error {
	var data string
	switch v := payload.(type) {
	case []byte:
		data = string(v)
	case string:
		data = v
	default:
		return fmt.Errorf("unsupported payload type: %T", payload)
	}

	msgID := metadata.GetID()
	if msgID == "" {
		msgID = m.generateStanzaID()
	}

	message := &stanza.Message{
		Attrs: stanza.Attrs{
			To:   target,
			From: m.jID,
			Type: stanza.StanzaType(metadata.GetType()),
			Id:   msgID,
		},
		Body:    data,
		Subject: metadata.Subject,
	}

	if err := m.client.Send(message); err != nil {
		return fmt.Errorf("failed to send message to %s: %w", target, err)
	}

	if m.logger.IsDebug() {
		m.logger.Debug("[%s] sent message stanza to %s (type: %s): %s", m.jID, target, message.Type, data)
	} else if m.logger.IsInfo() {
		m.logger.Info("[%s] sent message stanza to %s", m.jID, target)
	}

	return nil
}

func (m *Client) sendIQ(payload any, target string, metadata *xmpp.IQStanzaMetadata) error {
	var iqPayload stanza.IQPayload
	if payload != nil {
		if p, ok := payload.(stanza.IQPayload); ok {
			iqPayload = p
		}
	}

	iqID := metadata.GetID()
	if iqID == "" {
		iqID = m.generateStanzaID()
	}

	iq := &stanza.IQ{
		Attrs: stanza.Attrs{
			To:   target,
			From: m.jID,
			Type: stanza.StanzaType(metadata.GetType()),
			Id:   iqID,
		},
		Payload: iqPayload,
	}

	if err := m.client.Send(iq); err != nil {
		return fmt.Errorf("failed to send IQ to %s: %w", target, err)
	}

	if m.logger.IsDebug() {
		m.logger.Debug("[%s] sent IQ %s to %s (type: %s)", m.jID, iq.Id, target, iq.Type)
	} else if m.logger.IsInfo() {
		m.logger.Info("[%s] sent IQ %s to %s", m.jID, iq.Id, target)
	}

	return nil
}

func (m *Client) sendPresence(payload any, target string, metadata *xmpp.PresenceStanzaMetadata) error {
	// TODO: implement presence
	return nil
}

func (m *Client) IsConnected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isConnected
}

func (m *Client) Disconnect() error {
	m.mu.Lock()
	m.isShuttingDown = true
	wasConnected := m.isConnected
	m.isConnected = false
	m.mu.Unlock()

	if m.streamManager != nil && wasConnected {
		stopChan := make(chan struct{})
		go func() {
			m.streamManager.Stop()
			close(stopChan)
		}()

		select {
		case <-stopChan:
		case <-time.After(5 * time.Second):
			m.logger.Warn("[%s] stream manager stop timeout, forcing disconnect", m.jID)
		}
	}

	if m.client != nil {
		m.client.Disconnect()
	}

	m.iqResponses.Range(func(key, value any) bool {
		m.iqResponses.Delete(key)
		return true
	})

	m.logger.Info("[%s] disconnected", m.jID)
	metrics.DecConnection()
	return nil
}

func (m *Client) ID() string {
	return m.jID
}

var _ client.Client = (*Client)(nil)
