package behavior

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"mqtt-simulator/pkg/common"
)

type Behavior interface {
	OnConnect(ctx common.ClientContext) []common.Action
	OnMessage(ctx common.ClientContext, msg mqtt.Message) []common.Action
	OnTick(ctx common.ClientContext, tick int64) []common.Action
	OnDisconnect(ctx common.ClientContext)
}
