package common

type Message interface {
	Payload() string
	Metadata() map[string]any
}
