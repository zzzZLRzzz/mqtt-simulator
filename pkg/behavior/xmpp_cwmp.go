package behavior

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"

	"github.com/go-viper/mapstructure/v2"

	act "conn-conductor/pkg/action"
	xmppmeta "conn-conductor/pkg/action/xmpp"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/common"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/cwmp"
	"conn-conductor/pkg/logging"
)

type cwmpBehaviorConfig struct {
	ACSURL                    string   `mapstructure:"acs_url"`
	ConnectionRequestUsername string   `mapstructure:"connection_request_username"`
	ConnectionRequestPassword string   `mapstructure:"connection_request_password"`
	InformInterval            int64    `mapstructure:"inform_interval"`
	Manufacturer              string   `mapstructure:"manufacturer"`
	OUI                       string   `mapstructure:"oui"`
	ProductClass              string   `mapstructure:"product_class"`
	SerialNumber              string   `mapstructure:"serial_number"`
	DataModelVersion          string   `mapstructure:"data_model_version"`
	HardwareVersion           string   `mapstructure:"hardware_version"`
	SoftwareVersion           string   `mapstructure:"software_version"`
	ProvisioningCode          string   `mapstructure:"provisioning_code"`
	ParameterKey              string   `mapstructure:"parameter_key"`
	ConnectionRequestURL      string   `mapstructure:"connection_request_url"`
	AliasBasedAddressing      string   `mapstructure:"alias_based_addressing"`
	EventCodes                []string `mapstructure:"event_codes"`
	CommandKey                string   `mapstructure:"command_key"`
}

type CWMPBehavior struct {
	logger                    *logging.Logger
	informConfig              cwmp.InformConfig
	acsURL                    string
	connectionRequestUsername string
	connectionRequestPassword string
	informInterval            int64
	httpClient                *http.Client
}

func NewCWMPBehavior(cfg config.BehaviorConfig, logger *logging.Logger) Behavior {
	var cwmpCfg cwmpBehaviorConfig
	if cfg.Custom != nil {
		if err := mapstructure.Decode(cfg.Custom, &cwmpCfg); err != nil {
			logger.Warn("failed to decode CWMP custom config: %v, using defaults", err)
		}
	}

	informConfig := cwmp.InformConfig{
		MaxEnvelopes:         1,
		RetryCount:           0,
		Manufacturer:         cwmpCfg.Manufacturer,
		OUI:                  cwmpCfg.OUI,
		ProductClass:         cwmpCfg.ProductClass,
		SerialNumber:         cwmpCfg.SerialNumber,
		DataModelVersion:     cwmpCfg.DataModelVersion,
		HardwareVersion:      cwmpCfg.HardwareVersion,
		SoftwareVersion:      cwmpCfg.SoftwareVersion,
		ProvisioningCode:     cwmpCfg.ProvisioningCode,
		ParameterKey:         cwmpCfg.ParameterKey,
		ConnectionRequestURL: cwmpCfg.ConnectionRequestURL,
		AliasBasedAddressing: cwmpCfg.AliasBasedAddressing,
		EventCodes:           cwmpCfg.EventCodes,
		CommandKey:           cwmpCfg.CommandKey,
	}

	if cwmpCfg.InformInterval <= 0 {
		cwmpCfg.InformInterval = 300
	}

	return &CWMPBehavior{
		logger:                    logger,
		informConfig:              informConfig,
		acsURL:                    cwmpCfg.ACSURL,
		connectionRequestUsername: cwmpCfg.ConnectionRequestUsername,
		connectionRequestPassword: cwmpCfg.ConnectionRequestPassword,
		informInterval:            cwmpCfg.InformInterval,
		httpClient:                &http.Client{},
	}
}

func (b *CWMPBehavior) SupportedConnectors() []string {
	return []string{config.ConnectorTypeXMPP}
}

func (b *CWMPBehavior) OnConnect(client client.Client) []act.Action {
	clientID := client.ID()
	b.logger.Info("[%s] CWMP client connected to XMPP server", clientID)
	return nil
}

func (b *CWMPBehavior) OnMessage(client client.Client, msg common.Message) []act.Action {
	payload := msg.Payload()
	clientID := client.ID()
	metadata := msg.Metadata()

	kind, ok := metadata[xmppmeta.MetadataKeyStanzaKind].(string)
	if !ok || kind != xmppmeta.StanzaKindIQ {
		return nil
	}

	msgType, ok := metadata[xmppmeta.MetadataKeyType].(string)
	if !ok {
		return nil
	}

	if msgType == "get" && b.isConnectionRequest(payload) {
		return b.handleConnectionRequest(clientID, payload, metadata)
	}

	return nil
}

func (b *CWMPBehavior) OnTick(client client.Client, tick int64) []act.Action {
	clientID := client.ID()
	if tick%b.informInterval != 0 {
		return nil
	}

	b.logger.Info("[%s] sending periodic Inform (tick: %d)", clientID, tick)
	informConfig := b.informConfig
	informConfig.MsgID = cwmp.GenerateMsgID()
	informConfig.EventCodes = append(informConfig.EventCodes, "2 PERIODIC")
	go b.sendInformHTTP(clientID, informConfig)

	return nil
}

func (b *CWMPBehavior) OnDisconnect(client client.Client) {
}

func (b *CWMPBehavior) isConnectionRequest(payload string) bool {
	return len(payload) > 0 && (contains(payload, "ConnectionRequest") && contains(payload, "urn:dslforum-org:cwmp"))
}

func (b *CWMPBehavior) handleConnectionRequest(clientID string, payload string, metadata map[string]any) []act.Action {
	cr, err := b.parseConnectionRequest(payload)
	if err != nil {
		b.logger.Error("[%s] failed to parse ConnectionRequest: %v", clientID, err)
		return nil
	}

	from, _ := metadata[xmppmeta.MetadataKeyFrom].(string)
	msgID, _ := metadata[xmppmeta.MetadataKeyID].(string)

	b.logger.Info("[%s] received ConnectionRequest from ACS: %s", clientID, from)

	if !b.validateCredentials(cr.Username, cr.Password) {
		b.logger.Warn("[%s] ConnectionRequest authentication failed for user: %s", clientID, cr.Username)
		return []act.Action{b.buildConnectionRequestError(msgID, from)}
	}

	b.logger.Info("[%s] ConnectionRequest authenticated successfully", clientID)
	informConfig := b.informConfig
	informConfig.MsgID = cwmp.GenerateMsgID()
	informConfig.EventCodes = append(informConfig.EventCodes, "6 CONNECTION REQUEST")
	go b.sendInformHTTP(clientID, informConfig)

	return []act.Action{b.buildConnectionRequestResponse(msgID, from)}
}

func (b *CWMPBehavior) validateCredentials(username, password string) bool {
	return username == b.connectionRequestUsername && password == b.connectionRequestPassword
}

func (b *CWMPBehavior) buildConnectionRequestResponse(id, target string) act.Action {
	return &act.SendAction{
		Payload: nil,
		Target:  target,
		Metadata: (&xmppmeta.IQStanzaMetadata{
			XMPPBaseMetadata: xmppmeta.XMPPBaseMetadata{
				Kind: xmppmeta.StanzaKindIQ,
				To:   target,
				ID:   id,
			},
			Type: "result",
		}).ToMap(),
	}
}

func (b *CWMPBehavior) buildConnectionRequestError(id, target string) act.Action {
	return &act.SendAction{
		Payload: nil,
		Target:  target,
		Metadata: (&xmppmeta.IQStanzaMetadata{
			XMPPBaseMetadata: xmppmeta.XMPPBaseMetadata{
				Kind: xmppmeta.StanzaKindIQ,
				To:   target,
				ID:   id,
			},
			Type: "error",
		}).ToMap(),
	}
}

func (b *CWMPBehavior) parseConnectionRequest(payload string) (*cwmp.ConnectionRequest, error) {
	var cr cwmp.ConnectionRequest
	if err := xml.Unmarshal([]byte(payload), &cr); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ConnectionRequest: %w", err)
	}
	return &cr, nil
}

func (b *CWMPBehavior) sendInformHTTP(clientID string, informConfig cwmp.InformConfig) {
	if b.acsURL == "" {
		b.logger.Error("[%s] ACS URL not configured, cannot send Inform", clientID)
		return
	}

	envelope := cwmp.BuildStandardInform(informConfig)
	data, err := xml.Marshal(envelope)
	if err != nil {
		b.logger.Error("[%s] failed to marshal Inform: %v", clientID, err)
		return
	}

	b.logger.Info("[%s] sending Inform to ACS: %s", clientID, b.acsURL)

	req, err := http.NewRequest("POST", b.acsURL, bytes.NewBuffer(data))
	if err != nil {
		b.logger.Error("[%s] failed to create HTTP request: %v", clientID, err)
		return
	}

	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set("SOAPAction", "urn:dslforum-org:cwmp-1-0#Inform")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		b.logger.Error("[%s] failed to send Inform: %v", clientID, err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		b.logger.Error("[%s] failed to read response: %v", clientID, err)
		return
	}

	if resp.StatusCode == http.StatusOK {
		b.logger.Info("[%s] Inform sent successfully, response: %s", clientID, string(body))
	} else {
		b.logger.Warn("[%s] Inform failed with status %d: %s", clientID, resp.StatusCode, string(body))
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

var _ Behavior = (*CWMPBehavior)(nil)
