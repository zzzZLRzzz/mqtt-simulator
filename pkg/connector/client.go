package connector

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"mqtt-simulator/pkg/behavior"
	"mqtt-simulator/pkg/common"
	"mqtt-simulator/pkg/config"
	"mqtt-simulator/pkg/logging"
	"mqtt-simulator/pkg/metrics"
)

type ActionHandler func(ctx common.ClientContext, actions []common.Action)

type MQTTClient struct {
	clientID         string
	client           mqtt.Client
	broker           config.Broker
	logger           *logging.Logger
	creds            config.Creds
	behavior         behavior.Behavior
	actionHandler    ActionHandler
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

func NewMQTTClient(
	logger *logging.Logger,
	broker config.Broker,
	index int,
	creds config.Creds,
	beh behavior.Behavior,
	actionHandler ActionHandler,
) *MQTTClient {
	if beh == nil {
		panic("behavior cannot be nil")
	}
	if actionHandler == nil {
		panic("actionHandler cannot be nil")
	}

	m := &MQTTClient{
		clientID:      creds.ClientID,
		broker:        broker,
		logger:        logger,
		creds:         creds,
		behavior:      beh,
		actionHandler: actionHandler,
		subscriptions: make([]subscriptionInfo, 0),
		isReceiving:   true,
		stopReconnect: make(chan struct{}),
	}

	return m
}

func (m *MQTTClient) Connect() error {
	opts := mqtt.NewClientOptions()
	if err := connectConfiguration(m.broker, m.creds, opts); err != nil {
		return err
	}

	m.mu.Lock()
	m.isInitialConnect = true
	m.mu.Unlock()

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		m.logger.Info("[%s] connected to %s", m.clientID, m.broker.Address)
		m.mu.Lock()
		isInitial := m.isInitialConnect
		m.isInitialConnect = false
		m.mu.Unlock()

		if !isInitial {
			actions := m.behavior.OnConnect(m)
			m.actionHandler(m, actions)
		}
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
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

	opts.SetDefaultPublishHandler(func(c mqtt.Client, msg mqtt.Message) {
		m.mu.RLock()
		isReceiving := m.isReceiving
		m.mu.RUnlock()
		if !isReceiving {
			return
		}
		actions := m.behavior.OnMessage(m, msg)
		m.actionHandler(m, actions)
	})

	m.client = mqtt.NewClient(opts)
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect: %w", token.Error())
	}

	m.mu.Lock()
	m.isInitialConnect = false
	m.mu.Unlock()

	return nil
}

func (m *MQTTClient) StopReceiving() {
	m.mu.Lock()
	m.isReceiving = false
	m.mu.Unlock()
}

func connectConfiguration(broker config.Broker, creds config.Creds, opts *mqtt.ClientOptions) error {
	opts.AddBroker(broker.Address)
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

func (m *MQTTClient) reconnect() {
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

func (m *MQTTClient) Subscribe(topic string, qos byte) error {
	m.mu.Lock()
	for _, sub := range m.subscriptions {
		if sub.topic == topic {
			m.mu.Unlock()
			return nil
		}
	}
	m.mu.Unlock()

	token := m.client.Subscribe(topic, qos, nil)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to %s: %w", topic, token.Error())
	}

	m.mu.Lock()
	m.subscriptions = append(m.subscriptions, subscriptionInfo{topic: topic, qos: qos})
	m.mu.Unlock()

	m.logger.Info("[%s] subscribed to %s (QoS %d)", m.clientID, topic, qos)
	return nil
}

func (m *MQTTClient) Unsubscribe(topic string) error {
	token := m.client.Unsubscribe(topic)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to unsubscribe from %s: %w", topic, token.Error())
	}

	m.mu.Lock()
	for i, sub := range m.subscriptions {
		if sub.topic == topic {
			m.subscriptions = append(m.subscriptions[:i], m.subscriptions[i+1:]...)
			break
		}
	}
	m.mu.Unlock()

	m.logger.Info("[%s] unsubscribed from %s", m.clientID, topic)
	return nil
}

func (m *MQTTClient) Publish(topic string, qos byte, retain bool, payload interface{}) error {
	var data []byte
	switch v := payload.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("unsupported payload type: %T", payload)
	}

	token := m.client.Publish(topic, qos, retain, data)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish to %s: %w", topic, token.Error())
	}
	return nil
}

func (m *MQTTClient) IsConnected() bool {
	return m.client != nil && m.client.IsConnected()
}

func (m *MQTTClient) Disconnect() error {
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

func (m *MQTTClient) ClientID() string {
	return m.clientID
}

var _ common.ClientContext = (*MQTTClient)(nil)
