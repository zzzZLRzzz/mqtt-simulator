package cwmp

import (
	"fmt"
	"time"
)

type InformConfig struct {
	// DeviceId information
	Manufacturer string
	OUI          string
	ProductClass string
	SerialNumber string

	// Event information
	EventCodes []string // e.g.: ["0 BOOT", "1 BOOT"]
	CommandKey string   // Command key, required for some events

	// Forced parameters
	DataModelVersion     string // Device.RootDataModelVersion
	HardwareVersion      string // Device.DeviceInfo.HardwareVersion
	SoftwareVersion      string // Device.DeviceInfo.SoftwareVersion
	ProvisioningCode     string // Device.DeviceInfo.ProvisioningCode
	ParameterKey         string // Device.ManagementServer.ParameterKey
	ConnectionRequestURL string // Device.ManagementServer.ConnectionRequestURL
	AliasBasedAddressing string // Device.ManagementServer.AliasBasedAddressing ("0" or "1")

	// Others
	MaxEnvelopes int
	RetryCount   int
	MsgID        string

	// Extra parameters (optional)
	ExtraParams map[string]ParamValue
}

// ParamValue parameter value structure
type ParamValue struct {
	Text string
	Type string
}

func BuildStandardInform(config InformConfig) *SoapEnvelope {
	// Build DeviceId
	deviceID := DeviceID{
		Manufacturer: config.Manufacturer,
		OUI:          config.OUI,
		ProductClass: config.ProductClass,
		SerialNumber: config.SerialNumber,
	}

	// Build Event
	events := make([]EventStruct, len(config.EventCodes))
	for i, code := range config.EventCodes {
		events[i] = EventStruct{
			EventCode:  code,
			CommandKey: config.CommandKey, // Some events require CommandKey
		}
	}

	paramList := buildForcedParameters(config)

	// Build Inform
	inform := Inform{
		DeviceID:      deviceID,
		Event:         EventList{EventStructs: events},
		MaxEnvelopes:  config.MaxEnvelopes,
		CurrentTime:   time.Now().UTC().Format(time.RFC3339),
		RetryCount:    config.RetryCount,
		ParameterList: paramList,
	}

	// Build complete SOAP Envelope
	return &SoapEnvelope{
		Header: SoapHeader{
			ID: CWMPID{Text: config.MsgID},
		},
		Body: SoapBody{
			Inform: inform,
		},
	}
}

// TR-098: https://cwmp-data-models.broadband-forum.org/tr-098-1-2-0.html#forced-inform-parameters
// TR-181: https://cwmp-data-models.broadband-forum.org/tr-181-2-18-1-cwmp.html#forced-inform-parameters
func buildForcedParameters(config InformConfig) ParameterList {
	params := []ParameterValueStruct{
		{
			Name: "Device.RootDataModelVersion",
			Value: Value{
				Text: config.DataModelVersion,
				Type: "xsd:string",
			},
		},
		{
			Name: "Device.DeviceInfo.HardwareVersion",
			Value: Value{
				Text: config.HardwareVersion,
				Type: "xsd:string",
			},
		},
		{
			Name: "Device.DeviceInfo.SoftwareVersion",
			Value: Value{
				Text: config.SoftwareVersion,
				Type: "xsd:string",
			},
		},
		{
			Name: "Device.DeviceInfo.ProvisioningCode",
			Value: Value{
				Text: config.ProvisioningCode,
				Type: "xsd:string",
			},
		},
		{
			Name: "Device.ManagementServer.ParameterKey",
			Value: Value{
				Text: config.ParameterKey,
				Type: "xsd:string",
			},
		},
		{
			Name: "Device.ManagementServer.ConnectionRequestURL",
			Value: Value{
				Text: config.ConnectionRequestURL,
				Type: "xsd:string",
			},
		},
		{
			Name: "Device.ManagementServer.AliasBasedAddressing",
			Value: Value{
				Text: config.AliasBasedAddressing,
				Type: "xsd:boolean",
			},
		},
	}

	if config.ExtraParams != nil {
		for key, value := range config.ExtraParams {
			params = append(params, ParameterValueStruct{
				Name: key,
				Value: Value{
					Text: value.Text,
					Type: value.Type,
				},
			})
		}
	}

	return ParameterList{ParameterValueStructs: params}
}

func GenerateMsgID() string {
	return fmt.Sprintf("msg-%d-%d", time.Now().UnixNano(), time.Now().UnixNano()%10000)
}
