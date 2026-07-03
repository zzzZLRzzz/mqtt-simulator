package behavior

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"mqtt-simulator/pkg/common"
	"mqtt-simulator/pkg/config"
	"mqtt-simulator/pkg/logging"
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

func NewDeclarativeBehavior(cfg config.BehaviorConfig, logger *logging.Logger) *DeclarativeBehavior {
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

func (b *DeclarativeBehavior) OnConnect(ctx common.ClientContext) []common.Action {
	return b.actionsToCommonActions(b.config.OnConnect, ctx.ClientID(), "", "")
}

func (b *DeclarativeBehavior) OnMessage(ctx common.ClientContext, msg mqtt.Message) []common.Action {
	topic := msg.Topic()
	payload := string(msg.Payload())
	clientID := ctx.ClientID()

	if b.logger.IsInfo() {
		b.logger.Info("[%s] received message from %s", clientID, topic)
	}
	if b.logger.IsDebug() {
		b.logger.Debug("[%s] received message from %s: %s", clientID, topic, payload)
	}

	return b.actionsToCommonActions(b.config.OnMessage, clientID, topic, payload)
}

func (b *DeclarativeBehavior) OnTick(ctx common.ClientContext, tick int64) []common.Action {
	var actions []common.Action
	for _, ta := range b.timerActions {
		if tick%ta.interval == 0 {
			actions = append(actions, b.actionsToCommonActions([]config.BehaviorAction{ta.action}, ctx.ClientID(), "", "")...)
		}
	}

	return actions
}

func (b *DeclarativeBehavior) OnDisconnect(ctx common.ClientContext) {
}

func (b *DeclarativeBehavior) actionsToCommonActions(actions []config.BehaviorAction, clientID, messageTopic, messagePayload string) []common.Action {
	result := make([]common.Action, 0, len(actions))
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
			result = append(result, common.SubscribeAction{
				Topic: topic,
				QoS:   action.Subscribe.QoS,
			})
		}

		if action.Publish != nil {
			topic, err := config.ExecuteTemplate(action.Publish.Topic, templateData)
			if err != nil {
				b.logger.Error("[%s] failed to render publish topic: %v", clientID, err)
				continue
			}

			payload, err := config.ExecuteTemplate(action.Publish.Payload, templateData)
			if err != nil {
				b.logger.Error("[%s] failed to render publish payload: %v", clientID, err)
				continue
			}

			result = append(result, common.PublishAction{
				Topic:   topic,
				QoS:     action.Publish.QoS,
				Retain:  action.Publish.Retain,
				Payload: payload,
			})
		}

		if action.Unsubscribe != nil {
			topic, err := config.ExecuteTemplate(*action.Unsubscribe, templateData)
			if err != nil {
				b.logger.Error("[%s] failed to render unsubscribe topic: %v", clientID, err)
				continue
			}
			result = append(result, common.UnsubscribeAction{
				Topic: topic,
			})
		}

		if action.Disconnect {
			result = append(result, common.DisconnectAction{})
		}
	}

	return result
}

var _ Behavior = (*DeclarativeBehavior)(nil)
