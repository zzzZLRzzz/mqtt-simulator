package common

type Action interface {
}

type PublishAction struct {
	Topic   string
	QoS     byte
	Retain  bool
	Payload interface{}
}

type SubscribeAction struct {
	Topic string
	QoS   byte
}

type UnsubscribeAction struct {
	Topic string
}

type DisconnectAction struct {
}

type ClientContext interface {
	Publish(topic string, qos byte, retain bool, payload interface{}) error
	Subscribe(topic string, qos byte) error
	Unsubscribe(topic string) error
	Disconnect() error
	IsConnected() bool
	ClientID() string
}
