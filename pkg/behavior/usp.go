package behavior

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"mqtt-simulator/pkg/common"
	"mqtt-simulator/pkg/logging"
)

// USPBehavior implements USP/TR-369 protocol behavior
// TODO: Complete USP protocol implementation
type USPBehavior struct {
	logger *logging.Logger
}

func NewUSPBehavior(logger *logging.Logger) *USPBehavior {
	return &USPBehavior{
		logger: logger,
	}
}

func (b *USPBehavior) OnConnect(ctx common.ClientContext) []common.Action {
	return nil
}

func (b *USPBehavior) OnMessage(ctx common.ClientContext, msg mqtt.Message) []common.Action {
	topic := msg.Topic()
	payload := string(msg.Payload())
	clientID := ctx.ClientID()

	if b.logger.IsInfo() {
		b.logger.Info("[%s] received USP message from %s", clientID, topic)
	}
	if b.logger.IsDebug() {
		b.logger.Debug("[%s] received USP message from %s: %s", clientID, topic, payload)
	}

	// TODO: Handle USP messages
	// Parse incoming USP messages and generate appropriate responses
	return nil
}

func (b *USPBehavior) OnTick(ctx common.ClientContext, tick int64) []common.Action {
	// TODO: Implement periodic USP operations
	// - Send periodic heartbeat
	// - Check for pending operations
	return nil
}

func (b *USPBehavior) OnDisconnect(ctx common.ClientContext) {
	// TODO: Clean up USP state on disconnect
}

var _ Behavior = (*USPBehavior)(nil)
