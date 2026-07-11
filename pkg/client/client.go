package client

type Client interface {
	Connect() error
	Send(payload any, target string, metadata map[string]any) error
	Subscribe(target string, metadata map[string]any) error
	Unsubscribe(target string) error
	Disconnect() error
	IsConnected() bool
	ID() string
	StopReceiving()
}