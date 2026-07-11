package behavior

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"google.golang.org/protobuf/proto"

	act "conn-conductor/pkg/action"
	mqttmeta "conn-conductor/pkg/action/mqtt"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/common"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/logging"
	"conn-conductor/pkg/usp"
	"conn-conductor/pkg/usp/usp_record"
)

type uspBehaviorConfig struct {
	AgentID            string `mapstructure:"agent_id"`
	ControllerID       string `mapstructure:"controller_id"`
	ControllerTopicFmt string `mapstructure:"controller_topic_fmt"`
	AgentTopicFmt      string `mapstructure:"agent_topic_fmt"`
}

type USPBehavior struct {
	logger             *logging.Logger
	agentID            string
	controllerID       string
	controllerTopicFmt string
	agentTopicFmt      string
}

func NewUSPBehavior(cfg config.BehaviorConfig, logger *logging.Logger) Behavior {
	var uspCfg uspBehaviorConfig
	if cfg.Custom != nil {
		if err := mapstructure.Decode(cfg.Custom, &uspCfg); err != nil {
			logger.Warn("failed to decode USP custom config: %v, using defaults", err)
		}
	}

	if uspCfg.AgentID == "" {
		uspCfg.AgentID = "defaultAgentID"
	}
	if uspCfg.ControllerID == "" {
		uspCfg.ControllerID = "defaultControllerID"
	}
	if uspCfg.ControllerTopicFmt == "" {
		uspCfg.ControllerTopicFmt = "/usp/%s/Controller"
	}
	if uspCfg.AgentTopicFmt == "" {
		uspCfg.AgentTopicFmt = "/usp/%s/Agent"
	}

	return &USPBehavior{
		logger:             logger,
		agentID:            uspCfg.AgentID,
		controllerID:       uspCfg.ControllerID,
		controllerTopicFmt: uspCfg.ControllerTopicFmt,
		agentTopicFmt:      uspCfg.AgentTopicFmt,
	}
}

func formatTopic(fmtStr, id string) string {
	if strings.Contains(fmtStr, "%s") {
		return fmt.Sprintf(fmtStr, id)
	}
	return fmtStr
}

func (b *USPBehavior) OnConnect(client client.Client) []act.Action {
	clientID := client.ID()

	bootPayload, err := b.buildBootEvent()
	if err != nil {
		b.logger.Error("[%s] failed to build USP boot event: %v", clientID, err)
		return nil
	}

	target := formatTopic(b.controllerTopicFmt, b.controllerID)

	b.logger.Info("[%s] sending USP boot event to %s", clientID, target)
	if b.logger.IsDebug() {
		b.logger.Debug("[%s] boot payload length: %d bytes", clientID, len(bootPayload))
	}

	return []act.Action{
		&act.SendAction{
			Target:   target,
			Payload:  bootPayload,
			Metadata: (&mqttmeta.MQTTPublishMetadata{QoS: 1, Retain: false}).ToMap(),
		},
		&act.SubscribeAction{
			Target:   formatTopic(b.agentTopicFmt, b.agentID),
			Metadata: (&mqttmeta.MQTTSubscribeMetadata{QoS: 1}).ToMap(),
		},
	}
}

func (b *USPBehavior) OnMessage(client client.Client, msg common.Message) []act.Action {
	payload := msg.Payload()
	clientID := client.ID()

	msgMeta := mqttmeta.ParseMessageMetadata(msg.Metadata())
	topic := msgMeta.Topic

	if b.logger.IsInfo() {
		b.logger.Info("[%s] received USP message from %s, length: %d", clientID, topic, len(payload))
	}

	record, err := b.parseUSPRecord(payload)
	if err != nil {
		b.logger.Error("[%s] failed to parse USP record: %v", clientID, err)
		return nil
	}

	if b.logger.IsDebug() {
		b.logger.Debug("[%s] USP record from: %s, to: %s", clientID, record.FromId, record.ToId)
	}

	return nil
}

func (b *USPBehavior) OnTick(client client.Client, tick int64) []act.Action {
	return nil
}

func (b *USPBehavior) OnDisconnect(client client.Client) {
}

func (b *USPBehavior) buildBootEvent() ([]byte, error) {
	notifyMsg := &usp.Notify{
		SubscriptionId: "",
		SendResp:       true,
		Notification: &usp.Notify_Event_{
			Event: &usp.Notify_Event{
				ObjPath:   "Device.",
				EventName: "Boot!",
				Params: map[string]string{
					"CommandKey":      "",
					"Cause":           "LocalReboot",
					"FirmwareUpdated": "false",
					"ParameterMap":    "",
				},
			},
		},
	}

	uspMsg := &usp.Msg{
		Header: &usp.Header{
			MsgId:   generateMsgID(),
			MsgType: usp.Header_NOTIFY,
		},
		Body: &usp.Body{
			MsgBody: &usp.Body_Request{
				Request: &usp.Request{
					ReqType: &usp.Request_Notify{
						Notify: notifyMsg,
					},
				},
			},
		},
	}

	msgBytes, err := proto.Marshal(uspMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal USP message: %w", err)
	}

	record := &usp_record.Record{
		Version:         "1.5",
		ToId:            b.controllerID,
		FromId:          b.agentID,
		OriginatorId:    b.agentID,
		DestinationId:   b.controllerID,
		PayloadSecurity: usp_record.Record_PLAINTEXT,
		RecordType: &usp_record.Record_NoSessionContext{
			NoSessionContext: &usp_record.NoSessionContextRecord{
				Payload: msgBytes,
			},
		},
	}

	recordBytes, err := proto.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal USP record: %w", err)
	}

	return recordBytes, nil
}

func (b *USPBehavior) parseUSPRecord(payload []byte) (*usp_record.Record, error) {
	record := &usp_record.Record{}
	if err := proto.Unmarshal(payload, record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal USP record: %w", err)
	}
	return record, nil
}

func generateMsgID() string {
	return fmt.Sprintf("msg-%d", time.Now().UnixNano())
}

var _ Behavior = (*USPBehavior)(nil)
