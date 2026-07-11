package common

type Message interface {
	Payload() []byte
	Metadata() map[string]any
}