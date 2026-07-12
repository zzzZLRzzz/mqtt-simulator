package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	mqttlib "github.com/eclipse/paho.mqtt.golang"

	act "conn-conductor/pkg/action"
	mqttmeta "conn-conductor/pkg/action/mqtt"
	"conn-conductor/pkg/behavior"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/common"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/logging"
	"conn-conductor/pkg/metrics"
)

type Client struct {
	clientID         string
	client           mqttlib.Client
	broker           config.Broker
	logger           *logging.Logger
	creds            config.Creds
	behavior         behavior.Behavior
	actionHandler    act.ActionHandler
	subscriptions    []subscriptionInfo
	mu               sync.RWMutex
	isShuttingDown   bool
	isReceiving      bool
	isInitialConnect bool
	stopReconnect    chan struct{}
}

type subscriptionInfo struct {
	topic string
	qos   byte
}

type messageAdapter struct {
	msg mqttlib.Message
}

func (m *messageAdapter) Payload() []byte {
	return m.msg.Payload()
}

func (m *messageAdapter) Metadata() map[string]any {
	return (&mqttmeta.MQTTMessageMetadata{
		Topic: m.msg.Topic(),
		QoS:   m.msg.Qos(),
	}).ToMap()
}

var _ common.Message = (*messageAdapter)(nil)

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
		clientID:         creds.ClientID,
		broker:           broker,
		logger:           logger,
		creds:            creds,
		behavior:         beh,
		actionHandler:    actionHandler,
		subscriptions:    make([]subscriptionInfo, 0),
		isReceiving:      true,
		isInitialConnect: true,
		stopReconnect:    make(chan struct{}),
	}

	return m
}

func (m *Client) Connect() error {
	opts := mqttlib.NewClientOptions()
	if err := connectConfiguration(m.broker, m.creds, opts); err != nil {
		return err
	}

	opts.SetOnConnectHandler(func(c mqttlib.Client) {
		m.logger.Info("[%s] connected to %s", m.clientID, m.broker.Address)
		m.mu.Lock()
		m.isInitialConnect = false
		m.mu.Unlock()

		actions := m.behavior.OnConnect(m)
		m.actionHandler(m, actions)
	})

	opts.SetConnectionLostHandler(func(c mqttlib.Client, err error) {
		m.logger.Warn("[%s] connection lost: %v", m.clientID, err)
		m.mu.Lock()
		if m.isShuttingDown {
			m.mu.Unlock()
			return
		}
		m.subscriptions = make([]subscriptionInfo, 0)
		m.mu.Unlock()
		m.behavior.OnDisconnect(m)
		m.reconnect()
	})

	opts.SetDefaultPublishHandler(func(c mqttlib.Client, msg mqttlib.Message) {
		m.mu.RLock()
		isReceiving := m.isReceiving
		m.mu.RUnlock()
		if !isReceiving {
			return
		}

		metrics.MessagesReceived.WithLabelValues(msg.Topic()).Inc()
		metrics.IncReceived()

		actions := m.behavior.OnMessage(m, &messageAdapter{msg: msg})
		m.actionHandler(m, actions)
	})

	m.client = mqttlib.NewClient(opts)
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect: %w", token.Error())
	}

	m.mu.Lock()
	m.isInitialConnect = false
	m.mu.Unlock()

	return nil
}

func (m *Client) StopReceiving() {
	m.mu.Lock()
	m.isReceiving = false
	m.mu.Unlock()
}

func connectConfiguration(broker config.Broker, creds config.Creds, opts *mqttlib.ClientOptions) error {
	brokerAddr := broker.Address
	if !strings.Contains(brokerAddr, "://") {
		if broker.TLS {
			brokerAddr = "ssl://" + brokerAddr
		} else {
			brokerAddr = "tcp://" + brokerAddr
		}
	}
	opts.AddBroker(brokerAddr)
	opts.SetClientID(creds.ClientID)
	opts.SetUsername(creds.Username)
	opts.SetPassword(creds.Password)
	opts.SetKeepAlive(broker.Keepalive)
	opts.SetPingTimeout(time.Second)
	opts.SetConnectTimeout(broker.Timeout)

	if broker.TLS {
		tlsConfig := &tls.Config{}
		if broker.CAFile != "" {
			caCert, err := os.ReadFile(broker.CAFile)
			if err != nil {
				return fmt.Errorf("failed to read CA file: %w", err)
			}
			caPool := x509.NewCertPool()
			caPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caPool
		}
		if broker.CertFile != "" && broker.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(broker.CertFile, broker.KeyFile)
			if err != nil {
				return fmt.Errorf("failed to load cert/key: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		opts.SetTLSConfig(tlsConfig)
	}
	return nil
}

func (m *Client) reconnect() {
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-m.stopReconnect:
			m.logger.Info("[%s] reconnection cancelled", m.clientID)
			return
		case <-time.After(backoff):
		}

		m.logger.Info("[%s] attempting to reconnect...", m.clientID)
		if token := m.client.Connect(); token.Wait() && token.Error() == nil {
			m.logger.Info("[%s] reconnected successfully", m.clientID)
			return
		}

		m.logger.Warn("[%s] reconnection failed, retrying in %v", m.clientID, backoff*2)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (m *Client) Subscribe(target string, metadata map[string]any) error {
	m.mu.Lock()
	for _, sub := range m.subscriptions {
		if sub.topic == target {
			m.mu.Unlock()
			return nil
		}
	}
	m.mu.Unlock()

	mqttMeta := mqttmeta.ParseSubscribeMetadata(metadata)

	token := m.client.Subscribe(target, mqttMeta.QoS, nil)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", target, token.Error())
	}

	m.mu.Lock()
	m.subscriptions = append(m.subscriptions, subscriptionInfo{topic: target, qos: mqttMeta.QoS})
	m.mu.Unlock()

	m.logger.Info("[%s] subscribed to %s (QoS %d)", m.clientID, target, mqttMeta.QoS)
	return nil
}

func (m *Client) Unsubscribe(target string) error {
	token := m.client.Unsubscribe(target)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to unsubscribe from %s: %w", target, token.Error())
	}

	m.mu.Lock()
	for i, sub := range m.subscriptions {
		if sub.topic == target {
			m.subscriptions = append(m.subscriptions[:i], m.subscriptions[i+1:]...)
			break
		}
	}
	m.mu.Unlock()

	m.logger.Info("[%s] unsubscribed from %s", m.clientID, target)
	return nil
}

func (m *Client) Send(payload any, target string, metadata map[string]any) error {
	var data []byte
	switch v := payload.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("unsupported payload type: %T", payload)
	}

	mqttMeta := mqttmeta.ParsePublishMetadata(metadata)

	token := m.client.Publish(target, mqttMeta.QoS, mqttMeta.Retain, data)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish to %s: %w", target, token.Error())
	}

	if m.logger.IsDebug() {
		m.logger.Debug("[%s] published to %s (QoS %d, retain %v): %s", m.clientID, target, mqttMeta.QoS, mqttMeta.Retain, string(data))
	} else if m.logger.IsInfo() {
		m.logger.Info("[%s] published to %s (QoS %d)", m.clientID, target, mqttMeta.QoS)
	}

	return nil
}

func (m *Client) IsConnected() bool {
	return m.client != nil && m.client.IsConnected()
}

func (m *Client) Disconnect() error {
	m.mu.Lock()
	m.isShuttingDown = true
	m.mu.Unlock()

	select {
	case <-m.stopReconnect:
	default:
		close(m.stopReconnect)
	}

	m.client.Disconnect(250)
	m.logger.Info("[%s] disconnected", m.clientID)
	metrics.DecConnection()
	return nil
}

func (m *Client) ID() string {
	return m.clientID
}

var _ client.Client = (*Client)(nil)
