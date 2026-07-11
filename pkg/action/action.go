package action

import "conn-conductor/pkg/client"

type Action interface {
	Execute(client client.Client) error
}

type SendAction struct {
	Payload  any
	Target   string
	Metadata map[string]any
}

func (a *SendAction) Execute(client client.Client) error {
	return client.Send(a.Payload, a.Target, a.Metadata)
}

type SubscribeAction struct {
	Target   string
	Metadata map[string]any
}

func (a *SubscribeAction) Execute(client client.Client) error {
	return client.Subscribe(a.Target, a.Metadata)
}

type UnsubscribeAction struct {
	Target string
}

func (a *UnsubscribeAction) Execute(client client.Client) error {
	return client.Unsubscribe(a.Target)
}

type DisconnectAction struct {
}

func (a *DisconnectAction) Execute(client client.Client) error {
	return client.Disconnect()
}

type ActionHandler func(client client.Client, actions []Action)
