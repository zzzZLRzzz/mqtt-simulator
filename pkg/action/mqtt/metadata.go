package mqtt

const (
	MetadataKeyQoS    = "qos"
	MetadataKeyRetain = "retain"
)

type MQTTPublishMetadata struct {
	QoS    byte
	Retain bool
}

func ParsePublishMetadata(metadata map[string]any) *MQTTPublishMetadata {
	if metadata == nil {
		return &MQTTPublishMetadata{}
	}

	result := &MQTTPublishMetadata{}

	if q, ok := metadata[MetadataKeyQoS].(byte); ok {
		result.QoS = q
	} else if q, ok := metadata[MetadataKeyQoS].(int); ok {
		result.QoS = byte(q)
	}

	if r, ok := metadata[MetadataKeyRetain].(bool); ok {
		result.Retain = r
	}

	return result
}

func (m *MQTTPublishMetadata) ToMap() map[string]any {
	return map[string]any{
		MetadataKeyQoS:    m.QoS,
		MetadataKeyRetain: m.Retain,
	}
}

type MQTTSubscribeMetadata struct {
	QoS byte
}

func ParseSubscribeMetadata(metadata map[string]any) *MQTTSubscribeMetadata {
	if metadata == nil {
		return &MQTTSubscribeMetadata{}
	}

	result := &MQTTSubscribeMetadata{}

	if q, ok := metadata[MetadataKeyQoS].(byte); ok {
		result.QoS = q
	} else if q, ok := metadata[MetadataKeyQoS].(int); ok {
		result.QoS = byte(q)
	}

	return result
}

func (m *MQTTSubscribeMetadata) ToMap() map[string]any {
	return map[string]any{
		MetadataKeyQoS: m.QoS,
	}
}
