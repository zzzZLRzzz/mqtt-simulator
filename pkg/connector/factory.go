package connector

import (
	act "conn-conductor/pkg/action"
	"conn-conductor/pkg/behavior"
	"conn-conductor/pkg/client"
	mqttclient "conn-conductor/pkg/client/mqtt"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/logging"
)

type ConnectorFactory interface {
	CreateClient(index int, creds config.Creds, behavior behavior.Behavior, actionHandler act.ActionHandler) client.Client
}

type MQTTConnectorFactory struct {
	logger *logging.Logger
	broker config.Broker
}

func NewMQTTConnectorFactory(logger *logging.Logger, broker config.Broker) *MQTTConnectorFactory {
	return &MQTTConnectorFactory{
		logger: logger,
		broker: broker,
	}
}

func (f *MQTTConnectorFactory) CreateClient(index int, creds config.Creds, beh behavior.Behavior, actionHandler act.ActionHandler) client.Client {
	return mqttclient.NewClient(f.logger, f.broker, index, creds, beh, actionHandler)
}

var _ ConnectorFactory = (*MQTTConnectorFactory)(nil)
