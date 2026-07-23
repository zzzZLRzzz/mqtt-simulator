package behavior

import (
	act "conn-conductor/pkg/action"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/common"
)

type Behavior interface {
	SupportedConnectors() []string
	OnConnect(client client.Client) []act.Action
	OnMessage(client client.Client, msg common.Message) []act.Action
	OnTick(client client.Client, tick int64) []act.Action
	OnDisconnect(client client.Client)
}
