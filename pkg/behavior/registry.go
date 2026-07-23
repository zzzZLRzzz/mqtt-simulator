package behavior

import (
	"conn-conductor/pkg/config"
	"conn-conductor/pkg/logging"
)

type BehaviorFactory func(cfg config.BehaviorConfig, logger *logging.Logger) Behavior

var registry = make(map[string]BehaviorFactory)

func init() {
	Register(config.BehaviorModeDeclarative, NewDeclarativeBehavior)
	Register("mqtt_usp", NewUSPBehavior)
	Register("xmpp_cwmp", NewCWMPBehavior)
}

func Register(name string, factory BehaviorFactory) {
	registry[name] = factory
}

func NewBehavior(cfg config.BehaviorConfig, logger *logging.Logger) Behavior {
	if factory, ok := registry[cfg.Mode]; ok {
		return factory(cfg, logger)
	}
	return NewDeclarativeBehavior(cfg, logger)
}
