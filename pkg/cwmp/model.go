package cwmp

import "encoding/xml"

// SOAP Envelope
type SoapEnvelope struct {
	XMLName xml.Name `xml:"http://schemas.xmlsoap.org/soap/envelope/ Envelope"`
	Header  SoapHeader
	Body    SoapBody
}

// SOAP Header
type SoapHeader struct {
	XMLName xml.Name `xml:"http://schemas.xmlsoap.org/soap/envelope/ Header"`
	ID      CWMPID   `xml:"cwmp ID"`
}

// CWMP ID
type CWMPID struct {
	XMLName xml.Name `xml:"urn:dslforum-org:cwmp-1-0 ID"`
	Text    string   `xml:",chardata"`
}

// SOAP Body
type SoapBody struct {
	XMLName xml.Name `xml:"http://schemas.xmlsoap.org/soap/envelope/ Body"`
	Inform  Inform   `xml:"cwmp Inform"`
}

// Inform body
type Inform struct {
	XMLName       xml.Name      `xml:"urn:dslforum-org:cwmp-1-0 Inform"`
	DeviceID      DeviceID      `xml:"DeviceId"`
	Event         EventList     `xml:"Event"`
	MaxEnvelopes  int           `xml:"MaxEnvelopes"`
	CurrentTime   string        `xml:"CurrentTime"`
	RetryCount    int           `xml:"RetryCount"`
	ParameterList ParameterList `xml:"ParameterList,omitempty"`
}

// DeviceID structure
type DeviceID struct {
	Manufacturer string `xml:"Manufacturer"`
	OUI          string `xml:"OUI"`
	ProductClass string `xml:"ProductClass"`
	SerialNumber string `xml:"SerialNumber"`
}

// EventList structure
type EventList struct {
	EventStructs []EventStruct `xml:"EventStruct"`
}

// EventStruct structure
type EventStruct struct {
	EventCode  string `xml:"EventCode"`
	CommandKey string `xml:"CommandKey,omitempty"`
}

// ParameterList structure
type ParameterList struct {
	ParameterValueStructs []ParameterValueStruct `xml:"ParameterValueStruct,omitempty"`
}

// ParameterValueStruct structure
type ParameterValueStruct struct {
	Name  string `xml:"Name"`
	Value Value  `xml:"Value"`
}

// Value structure
type Value struct {
	XMLName xml.Name `xml:"Value"`
	Text    string   `xml:",chardata"`
	Type    string   `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
}

// ConnectionRequest CWMP ConnectionRequest IQ payload
type ConnectionRequest struct {
	XMLName  xml.Name `xml:"urn:dslforum-org:cwmp-1-0 ConnectionRequest"`
	Username string   `xml:"Username"`
	Password string   `xml:"Password"`
}
