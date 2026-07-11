package behavior

import (
	act "conn-conductor/pkg/action"
	mqttmeta "conn-conductor/pkg/action/mqtt"
	"conn-conductor/pkg/client"
	"conn-conductor/pkg/common"
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/logging"
)

type timerAction struct {
	action   config.BehaviorAction
	interval int64
}

type DeclarativeBehavior struct {
	config       config.BehaviorConfig
	logger       *logging.Logger
	timerActions []timerAction
}

func NewDeclarativeBehavior(cfg config.BehaviorConfig, logger *logging.Logger) Behavior {
	b := &DeclarativeBehavior{
		config: cfg,
		logger: logger,
	}
	b.initTimerActions()
	return b
}

func (b *DeclarativeBehavior) initTimerActions() {
	for _, action := range b.config.OnTimer {
		interval := action.Interval
		if interval <= 0 {
			interval = 1
		}
		b.timerActions = append(b.timerActions, timerAction{
			action:   action,
			interval: interval,
		})
	}
}

func (b *DeclarativeBehavior) OnConnect(client client.Client) []act.Action {
	return b.actionsToCommonActions(b.config.OnConnect, client.ID(), "", "")
}

func (b *DeclarativeBehavior) OnMessage(client client.Client, msg common.Message) []act.Action {
	payload := string(msg.Payload())
	clientID := client.ID()

	msgMeta := mqttmeta.ParseMessageMetadata(msg.Metadata())
	topic := msgMeta.Topic

	if b.logger.IsInfo() {
		b.logger.Info("[%s] received message from %s", clientID, topic)
	}
	if b.logger.IsDebug() {
		b.logger.Debug("[%s] received message from %s: %s", clientID, topic, payload)
	}

	return b.actionsToCommonActions(b.config.OnMessage, clientID, topic, payload)
}

func (b *DeclarativeBehavior) OnTick(client client.Client, tick int64) []act.Action {
	var actions []act.Action
	for _, ta := range b.timerActions {
		if tick%ta.interval == 0 {
			actions = append(actions, b.actionsToCommonActions([]config.BehaviorAction{ta.action}, client.ID(), "", "")...)
		}
	}

	return actions
}

func (b *DeclarativeBehavior) OnDisconnect(client client.Client) {
}

func (b *DeclarativeBehavior) actionsToCommonActions(actions []config.BehaviorAction, clientID, messageTopic, messagePayload string) []act.Action {
	result := make([]act.Action, 0, len(actions))
	templateData := config.TemplateData{
		ClientID:       clientID,
		MessageTopic:   messageTopic,
		MessagePayload: messagePayload,
	}

	for _, action := range actions {
		if action.Subscribe != nil {
			topic, err := config.ExecuteTemplate(action.Subscribe.Topic, templateData)
			if err != nil {
				b.logger.Error("[%s] failed to render subscribe topic: %v", clientID, err)
				continue
			}
			result = append(result, &act.SubscribeAction{
				Target:   topic,
				Metadata: (&mqttmeta.MQTTSubscribeMetadata{QoS: action.Subscribe.QoS}).ToMap(),
			})
		}

		if action.Send != nil {
			target, err := config.ExecuteTemplate(action.Send.Target, templateData)
			if err != nil {
				b.logger.Error("[%s] failed to render send target: %v", clientID, err)
				continue
			}

			payload, err := config.ExecuteTemplate(action.Send.Payload, templateData)
			if err != nil {
				b.logger.Error("[%s] failed to render send payload: %v", clientID, err)
				continue
			}

			result = append(result, &act.SendAction{
				Target:   target,
				Payload:  payload,
				Metadata: (&mqttmeta.MQTTPublishMetadata{QoS: action.Send.QoS, Retain: action.Send.Retain}).ToMap(),
			})
		}

		if action.Unsubscribe != nil {
			topic, err := config.ExecuteTemplate(*action.Unsubscribe, templateData)
			if err != nil {
				b.logger.Error("[%s] failed to render unsubscribe topic: %v", clientID, err)
				continue
			}
			result = append(result, &act.UnsubscribeAction{
				Target: topic,
			})
		}

		if action.Disconnect {
			result = append(result, &act.DisconnectAction{})
		}
	}

	return result
}

var _ Behavior = (*DeclarativeBehavior)(nil)
