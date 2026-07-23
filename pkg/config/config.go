package config

import (
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type Config struct {
	LogLevel string         `mapstructure:"log_level"`
	Engine   EngineConfig   `mapstructure:"engine" validate:"required"`
	Behavior BehaviorConfig `mapstructure:"behavior" validate:"required"`
	Metrics  Metrics        `mapstructure:"metrics"`
}

type EngineConfig struct {
	Broker          Broker           `mapstructure:"broker" validate:"required"`
	Credentials     Creds            `mapstructure:"credentials"`
	Connections     int              `mapstructure:"connections" validate:"min=1"`
	Connector       string           `mapstructure:"connector"`
	EnableRateLimit bool             `mapstructure:"enable_rate_limit"`
	RateLimits      RateLimitsConfig `mapstructure:"rate_limits"`
}

type RateLimitsConfig struct {
	Connect    RateLimitConfig `mapstructure:"connect"`
	Subscribe  RateLimitConfig `mapstructure:"subscribe"`
	Send       RateLimitConfig `mapstructure:"send"`
	Disconnect RateLimitConfig `mapstructure:"disconnect"`
}

type Broker struct {
	Address   string        `mapstructure:"address" validate:"required"`
	Keepalive time.Duration `mapstructure:"keepalive" validate:"min=5"`
	Timeout   time.Duration `mapstructure:"timeout"`
	CAFile    string        `mapstructure:"ca_file"`
	CertFile  string        `mapstructure:"cert_file"`
	KeyFile   string        `mapstructure:"key_file"`
	TLS       bool          `mapstructure:"tls"`
}

type Creds struct {
	ClientIDPrefix string `mapstructure:"client_id_prefix"`
	UsernamePrefix string `mapstructure:"username_prefix"`
	PasswordPrefix string `mapstructure:"password_prefix"`
	ClientID       string `mapstructure:"client_id"`
	Username       string `mapstructure:"username"`
	Password       string `mapstructure:"password"`
}

type Metrics struct {
	Enable         bool `mapstructure:"enable"`
	PrometheusPort int  `mapstructure:"prometheus_port"`
}

type RateLimitConfig struct {
	Rate  int `mapstructure:"rate"`
	Burst int `mapstructure:"burst"`
}

type BehaviorConfig struct {
	Mode      string           `mapstructure:"mode"`
	OnConnect []BehaviorAction `mapstructure:"on_connect"`
	OnTimer   []BehaviorAction `mapstructure:"on_timer"`
	OnMessage []BehaviorAction `mapstructure:"on_message"`
	Custom    map[string]any   `mapstructure:"custom"`
}

type BehaviorAction struct {
	Subscribe   *SubscribeActionConfig `mapstructure:"subscribe"`
	Send        *SendActionConfig      `mapstructure:"send"`
	Unsubscribe *string                `mapstructure:"unsubscribe"`
	Disconnect  bool                   `mapstructure:"disconnect"`
	Interval    int64                  `mapstructure:"interval"`
}

type SubscribeActionConfig struct {
	Topic string `mapstructure:"topic" validate:"required"`
	QoS   byte   `mapstructure:"qos" validate:"min=0,max=2"`
}

type SendActionConfig struct {
	Target  string `mapstructure:"target" validate:"required"`
	Payload string `mapstructure:"payload"`
	QoS     byte   `mapstructure:"qos" validate:"min=0,max=2"`
	Retain  bool   `mapstructure:"retain"`
}

const (
	BehaviorModeDeclarative = "declarative"
)

const (
	ConnectorTypeMQTT = "mqtt"
	ConnectorTypeXMPP = "xmpp"
)

var ConnectorTypes = [...]string{ConnectorTypeMQTT, ConnectorTypeXMPP}

func LoadConfig(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	validate := validator.New()

	if err := validate.StructPartial(config.Engine, "Broker", "Connections"); err != nil {
		return nil, fmt.Errorf("engine config validation failed: %w", err)
	}

	for i, action := range config.Behavior.OnConnect {
		if action.Subscribe != nil {
			if err := validate.Struct(action.Subscribe); err != nil {
				return nil, fmt.Errorf("on_connect[%d].subscribe validation failed: %w", i, err)
			}
		}
		if action.Send != nil {
			if err := validate.Struct(action.Send); err != nil {
				return nil, fmt.Errorf("on_connect[%d].send validation failed: %w", i, err)
			}
		}
	}

	for i, action := range config.Behavior.OnTimer {
		if action.Send != nil {
			if err := validate.Struct(action.Send); err != nil {
				return nil, fmt.Errorf("on_timer[%d].send validation failed: %w", i, err)
			}
		}
	}

	for i, action := range config.Behavior.OnMessage {
		if action.Subscribe != nil {
			if err := validate.Struct(action.Subscribe); err != nil {
				return nil, fmt.Errorf("on_message[%d].subscribe validation failed: %w", i, err)
			}
		}
		if action.Send != nil {
			if err := validate.Struct(action.Send); err != nil {
				return nil, fmt.Errorf("on_message[%d].send validation failed: %w", i, err)
			}
		}
	}

	if config.Behavior.Mode == "" {
		config.Behavior.Mode = BehaviorModeDeclarative
	}

	return &config, nil
}
