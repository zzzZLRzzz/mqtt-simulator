# MQTT Simulator
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A configurable MQTT client simulator for testing and benchmarking MQTT brokers.

## Features

- **Declarative Behavior**: Define client behavior through YAML configuration
- **Custom Behavior**: Implement custom behavior via the `Behavior` interface
- **Rate Limiting**: Configurable rate limits for connect, subscribe, publish, and disconnect operations
- **Metrics**: Built-in Prometheus metrics for monitoring
- **Template Support**: Go template support for dynamic topic and payload generation
- **Connection Pooling**: Efficient connection management with automatic reconnection

## Use Cases

- **Broker stress testing**: Simulate thousands of publishers/subscribers
- **Protocol development**: Implement custom behavior for USP/TR-369, LwM2M, etc.
- **Integration testing**: Verify your IoT platform handles real MQTT traffic
- **Benchmarking**: Measure broker throughput and latency

## Getting Started

### Prerequisites

- Go 1.21+

### Installation

```bash
go build -o mqtt-simulator ./cmd/mqtt-simulator
```

### Running

```bash
./mqtt-simulator --config examples/publisher.yaml
```

## Configuration

### Example: Publisher

```yaml
log_level: info
metrics:
  enable: true
  prometheus_port: 9090
engine:
  broker:
    address: tcp://broker.emqx.io:1883
    keepalive: 60
    timeout: 30s
  credentials:
    client_id_prefix: "publisher-"
  connections: 3
  enable_rate_limit: true
  rate_limits:
    publish:
      rate: 100
      burst: 200
behavior:
  on_timer:
    - interval: 5
      publish:
        topic: "devices/{{.ClientID}}/data"
        payload: |
          {
            "temperature": {{RandomFloat 20.0 45.0}},
            "humidity": {{RandomInt 30 90}},
            "timestamp": {{NowUnix}}
          }
        qos: 1
```

### Example: Subscriber

```yaml
log_level: debug
metrics:
  enable: true
  prometheus_port: 9090
engine:
  broker:
    address: tcp://broker.emqx.io:1883
    keepalive: 60
    timeout: 30s
  credentials:
    client_id_prefix: "subscriber-"
  connections: 2
behavior:
  on_connect:
    - subscribe:
        topic: "devices/+/data"
        qos: 1
```

### Example: Custom Mode (USP/TR-369)

```yaml
log_level: debug
engine:
  broker:
    address: tcp://localhost:1883
  credentials:
    client_id: "device-001"
  connections: 1
behavior:
  mode: custom
```

When `mode: custom` is set, the simulator uses the built-in `USPBehavior`. This is a placeholder implementation for USP/TR-369 protocol behavior. Currently, `OnConnect` returns no actions. For production use, implement your own custom behavior.

## Behavior Configuration

### Actions

| Action | Description |
|--------|-------------|
| `subscribe` | Subscribe to a topic |
| `publish` | Publish a message |
| `unsubscribe` | Unsubscribe from a topic |
| `disconnect` | Disconnect the client |

### Template Functions

| Variable/Function | Description |
|-------------------|-------------|
| `{{.ClientID}}` | Current client ID |
| `{{RandomInt min max}}` | Random integer in range [min, max] |
| `{{RandomFloat min max}}` | Random float in range [min, max] |
| `{{NowUnix}}` | Current Unix timestamp |

## Custom Behavior Implementation

To implement custom behavior, implement the `behavior.Behavior` interface:

```go
type MyCustomBehavior struct {
    logger *logging.Logger
}

func (b *MyCustomBehavior) OnConnect(ctx common.ClientContext) []common.Action {
    // Actions to perform on connect
    return []common.Action{
        common.SubscribeAction{Topic: "my/topic", QoS: 1},
    }
}

func (b *MyCustomBehavior) OnMessage(ctx common.ClientContext, msg mqtt.Message) []common.Action {
    // Actions to perform when a message is received
    return nil
}

func (b *MyCustomBehavior) OnTick(ctx common.ClientContext, tick int64) []common.Action {
    // Actions to perform on each tick (every second)
    return nil
}

func (b *MyCustomBehavior) OnDisconnect(ctx common.ClientContext) {
    // Cleanup on disconnect
}
```

## Metrics

Access metrics at `http://localhost:9090/metrics`:

| Metric | Description |
|--------|-------------|
| `mqtt_connections_total` | Total number of connections |
| `mqtt_connections_active` | Number of active connections |
| `mqtt_connections_failed` | Number of failed connections |
| `mqtt_messages_published` | Number of published messages |
| `mqtt_messages_received` | Number of received messages |
| `mqtt_messages_failed` | Number of failed messages |
| `mqtt_publish_latency_seconds` | Publish latency histogram |

## Project Structure

```
mqtt-simulator/
├── cmd/mqtt-simulator/     # Main entry point
├── pkg/
│   ├── behavior/           # Behavior implementations
│   │   ├── declarative.go  # Declarative behavior (YAML-driven)
│   │   ├── usp.go          # custom USP/TR-369 behavior
│   │   └── behavior.go     # Package entry
│   ├── common/             # Common interfaces
│   │   └── interfaces.go   # Action types and ClientContext interface
│   ├── config/             # Configuration handling
│   │   ├── config.go       # Config structs
│   │   └── template.go     # Template functions
│   ├── connector/          # MQTT connection management
│   │   ├── client.go       # MQTT client implementation
│   │   └── pool.go         # Connection pool
│   ├── engine/             # Core engine
│   │   └── engine.go       # Engine implementation
│   ├── generator/          # Credential generation
│   │   └── credential.go   # Client ID/username/password generation
│   ├── logging/            # Logging utilities
│   │   └── logging.go      # Logger implementation
│   └── metrics/            # Prometheus metrics
│       └── metrics.go      # Metrics server
└── examples/               # Example configurations
    ├── publisher.yaml
    ├── subscriber.yaml
    └── device.yaml
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Engine                                                          │
│ ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐ │
│ │ Worker Pool      │  │ Global Ticker    │  │ Rate Limiters   │ │
│ │ (10 Workers)     │  │ (1 sec interval) │  │ Connect/Sub     │ │
│ │ (channel per     │  │                  │  │ Pub/Disconn     │ │
│ │   worker)        │  │                  │  │                 │ │
│ └────────┬─────────┘  └────────┬─────────┘  └────────┬────────┘ │
│          │                     │                     │          │
│ ┌────────▼─────────────────────▼─────────────────────▼────────┐ │
│ │ Action Queue (sharded by ClientID hash)                     │ │
│ └─────────────────────────────┬───────────────────────────────┘ │
└───────────────────────────────┼─────────────────────────────────┘
                                │ SubmitActions()
┌───────────────────────────────▼─────────────────────────────────┐
│ MQTTClient (per connection)                                     │
│ ┌──────────────────────────────────────────────────────────────┐ │
│ │ ClientContext Interface                                     │ │
│ │ - Publish / Subscribe / Unsubscribe / Disconnect             │ │
│ │ - IsConnected / ClientID                                     │ │
│ └──────────────────────────────────────────────────────────────┘ │
│ ┌──────────────────────────────────────────────────────────────┐ │
│ │ paho.mqtt.golang (underlying MQTT client)                   │ │
│ └──────────────────────────────────────────────────────────────┘ │
│ ┌──────────────────────────────────────────────────────────────┐ │
│ │ Event Handlers → Behavior callbacks                          │ │
│ │ - OnConnectHandler → Behavior.OnConnect()                    │ │
│ │ - ConnectionLostHandler → Behavior.OnDisconnect()            │ │
│ │ - DefaultPublishHandler → Behavior.OnMessage()               │ │
│ └──────────────────────────────────────────────────────────────┘ │
└───────────────────────────────┬─────────────────────────────────┘
                                │ calls
┌───────────────────────────────▼─────────────────────────────────┐
│ Behavior                                                        │
│ ┌──────────────────────┐  ┌──────────────────────────────────┐ │
│ │ DeclarativeBehavior  │  │ CustomBehavior (USP, etc.)       │ │
│ │ (YAML-driven)        │  │ (Go code implementation)         │ │
│ └───────────┬──────────┘  └─────────────────┬────────────────┘ │
│             │                               │                  │
│ ┌───────────▼───────────────────────────────▼────────────────┐ │
│ │ OnConnect / OnMessage / OnTick / OnDisconnect              │ │
│ │ Returns []Action (Publish/Subscribe/Unsubscribe/Disconnect)│ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Description |
|-----------|-------------|
| **Engine** | Core orchestrator that manages workers, rate limiting, and global timer |
| **Worker Pool** | 10 concurrent workers that dequeue and execute actions from sharded queues |
| **Action Queue** | Sharded by ClientID hash to ensure sequential execution per client |
| **Global Ticker** | Broadcasts tick events to all clients every 1 second |
| **Rate Limiters** | Token bucket rate limiters for connect, subscribe, publish, and disconnect |
| **MQTTClient** | Wrapper around paho.mqtt.golang with reconnect logic and event handlers |
| **Behavior** | Interface defining client behavior (OnConnect, OnMessage, OnTick, OnDisconnect) |

## Data Flow

### Timer-Driven Actions

```
GlobalTicker fires every 1 second
        ↓
    Engine.broadcastTimerTick(tick)
        ↓
    For each connected client:
        Behavior.OnTick(client, tick) → returns []Action
        Engine.SubmitActions(client, actions)
            ↓
        Action goes to sharded queue (hash by ClientID)
            ↓
        Worker dequeues action
            ↓
        Engine.executeActions() → dispatches to specific handler
            ↓
        executePublishAction() / executeSubscribeAction() etc.
            ↓
        MQTTClient.Publish() / MQTTClient.Subscribe() etc.
            ↓
        paho.mqtt.golang publishes/subscribes to broker
```

### Message-Driven Actions

```
MQTT broker publishes message
        ↓
    MQTTClient.DefaultPublishHandler receives message
        ↓
    Behavior.OnMessage(client, msg) → returns []Action
        ↓
    actionHandler() → Engine.SubmitActions()
        ↓
    Action goes to sharded queue
        ↓
    Worker executes action (same as above)
```