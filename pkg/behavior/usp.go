package behavior

import (
	act "conn-conductor/pkg/action"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/common"
	"conn-conductor/pkg/logging"
)

type USPBehavior struct {
	logger *logging.Logger
}

func NewUSPBehavior(logger *logging.Logger) *USPBehavior {
	return &USPBehavior{
		logger: logger,
	}
}

func (b *USPBehavior) OnConnect(client client.Client) []act.Action {
	return nil
}

func (b *USPBehavior) OnMessage(client client.Client, msg common.Message) []act.Action {
	payload := string(msg.Payload())
	clientID := client.ID()

	metadata := msg.Metadata()
	topic := ""
	if t, ok := metadata["topic"].(string); ok {
		topic = t
	}

	if b.logger.IsInfo() {
		b.logger.Info("[%s] received USP message from %s", clientID, topic)
	}
	if b.logger.IsDebug() {
		b.logger.Debug("[%s] received USP message from %s: %s", clientID, topic, payload)
	}

	return nil
}

func (b *USPBehavior) OnTick(client client.Client, tick int64) []act.Action {
	return nil
}

func (b *USPBehavior) OnDisconnect(client client.Client) {
}

var _ Behavior = (*USPBehavior)(nil)
