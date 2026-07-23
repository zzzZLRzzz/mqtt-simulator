# Conn-Conductor
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A configurable connection conductor for managing and simulating long-lived connections (MQTT, XMPP, etc.).

## Features

- **Protocol Agnostic**: Supports multiple protocols via plugin architecture
- **Declarative Behavior**: Define client behavior through YAML configuration
- **Custom Behavior**: Implement custom behavior via the `Behavior` interface with registry pattern
- **USP/TR-369 Support**: Built-in support for USP/TR-369 protocol with boot event notification
- **CWMP/TR-069 Support**: Built-in support for CWMP (CPE WAN Management Protocol) with ConnectionRequest handling and periodic Inform sending
- **Rate Limiting**: Configurable rate limits for connect, subscribe, send, and disconnect operations
- **Metrics**: Built-in Prometheus metrics for monitoring
- **Template Support**: Go template support for dynamic target and payload generation
- **Connection Pooling**: Efficient connection management with automatic reconnection

## Use Cases

- **Broker stress testing**: Simulate thousands of publishers/subscribers
- **Protocol development**: Implement custom behavior for USP/TR-369, LwM2M, etc.
- **Integration testing**: Verify your IoT platform handles real traffic
- **Benchmarking**: Measure broker throughput and latency

## Getting Started

### Prerequisites

- Go 1.21+

### Installation

```bash
go build -o conn-conductor ./cmd/conn-conductor
```

### Running

```bash
./conn-conductor --config examples/mqtt_publisher.yaml
```

## Configuration

### Example: MQTT Publisher

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
    send:
      rate: 100
      burst: 200
behavior:
  on_timer:
    - interval: 5
      send:
        target: "devices/{{.ClientID}}/data"
        payload: |
          {
            "temperature": {{RandomFloat 20.0 45.0}},
            "humidity": {{RandomInt 30 90}},
            "timestamp": {{NowUnix}}
          }
        qos: 1
        retain: false
```

### Example: MQTT Subscriber

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
        target: "devices/+/data"
        qos: 1
```

### Example: USP/TR-369 Boot Event

```yaml
log_level: debug
metrics:
  enable: true
  prometheus_port: 9090
engine:
  broker:
    address: tcp://localhost:1883
    keepalive: 60
    timeout: 30s
  credentials:
    client_id_prefix: "usp-agent-"
  connections: 3
behavior:
  mode: usp
  custom:
    agent_id: "agent-001"
    controller_id: "controller-001"
    topic_prefix: "/usp/endpoint"
```

This configuration enables USP/TR-369 protocol behavior, which sends a Boot event notification on MQTT connect. The `custom` section allows passing behavior-specific configuration parameters.

### Example: CWMP/TR-069 Inform

```yaml
log_level: info
metrics:
  enable: true
  prometheus_port: 9090
engine:
  connector: xmpp
  broker:
    address: xmpp.example.com:5222
    tls: true
    keepalive: 60s
    timeout: 30s
  credentials:
    client_id: "cpe-001@example.com"
    password: "secret"
  connections: 1
behavior:
  mode: xmpp_cwmp
  custom:
    acs_url: "https://acs.example.com"
    connection_request_username: "admin"
    connection_request_password: "password"
    inform_interval: 300
    manufacturer: "Vendor"
    oui: "000BDB"
    product_class: "Product"
    serial_number: "SN12345"
    data_model_version: "1.0"
    hardware_version: "V1.0"
    software_version: "1.0.0"
    provisioning_code: ""
    parameter_key: ""
    connection_request_url: ""
    alias_based_addressing: "0"
```

This configuration enables CWMP/TR-069 protocol behavior for CPE (Customer Premises Equipment) simulation:
- **XMPP Connection**: Used for receiving ConnectionRequest IQ from ACS (Auto Configuration Server)
- **HTTP Inform**: Used for sending SOAP Inform requests to ACS
- **Periodic Inform**: Sends Inform at configurable intervals (`inform_interval` in seconds)
- **ConnectionRequest Handling**: Authenticates and responds to ACS ConnectionRequest IQ get

### Example: Custom Mode

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

When `mode: custom` is set, the simulator uses the built-in `USPBehavior`. For production use, implement your own custom behavior via the Behavior interface.

## Behavior Configuration

### Actions

| Action | Description |
|--------|-------------|
| `subscribe` | Subscribe to a target |
| `send` | Send a message to a target |
| `unsubscribe` | Unsubscribe from a target |
| `disconnect` | Disconnect the client |

### Template Functions

| Variable/Function | Description |
|-------------------|-------------|
| `{{.ClientID}}` | Current client ID |
| `{{RandomInt min max}}` | Random integer in range [min, max] |
| `{{RandomFloat min max}}` | Random float in range [min, max] |
| `{{NowUnix}}` | Current Unix timestamp |

## Custom Behavior Implementation

### Step 1: Implement the Behavior Interface

```go
type MyCustomBehavior struct {
    logger      *logging.Logger
    customParam string
}

func NewMyCustomBehavior(cfg config.BehaviorConfig, logger *logging.Logger) behavior.Behavior {
    b := &MyCustomBehavior{
        logger:      logger,
        customParam: "default",
    }
    
    if cfg.Custom != nil {
        if v, ok := cfg.Custom["custom_param"]; ok {
            b.customParam = fmt.Sprintf("%v", v)
        }
    }
    
    return b
}

func (b *MyCustomBehavior) OnConnect(client client.Client) []action.Action {
    return []action.Action{
        &action.SubscribeAction{
            Target:   "my/topic",
            Metadata: map[string]any{"qos": byte(1)},
        },
    }
}

func (b *MyCustomBehavior) OnMessage(client client.Client, msg common.Message) []action.Action {
    return nil
}

func (b *MyCustomBehavior) OnTick(client client.Client, tick int64) []action.Action {
    return nil
}

func (b *MyCustomBehavior) OnDisconnect(client client.Client) {
}

var _ behavior.Behavior = (*MyCustomBehavior)(nil)
```

### Step 2: Register the Behavior

In your main.go or init() function:

```go
func init() {
    behavior.Register("my_custom", NewMyCustomBehavior)
}
```

### Step 3: Configure in YAML

```yaml
behavior:
  mode: my_custom
  custom:
    custom_param: "my_value"
```

### Behavior Registry

The behavior registry allows dynamic behavior selection based on the `mode` field in the configuration. The following modes are built-in:

| Mode | Behavior | Description |
|------|----------|-------------|
| `declarative` | `DeclarativeBehavior` | YAML-driven behavior |
| `custom` | `USPBehavior` | USP/TR-369 behavior |
| `usp` | `USPBehavior` | USP/TR-369 behavior (alias) |
| `xmpp_cwmp` | `CWMPBehavior` | CWMP/TR-069 behavior with ConnectionRequest handling and periodic Inform |

Custom behaviors can be registered using `behavior.Register(name, factory)`.

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
conn-conductor/
├── cmd/conn-conductor/     # Main entry point
├── pkg/
│   ├── action/             # Action definitions
│   │   ├── action.go       # Generic action interfaces
│   │   ├── mqtt/           # MQTT-specific metadata
│   │   │   └── metadata.go # MQTT publish/subscribe metadata
│   │   └── xmpp/           # XMPP-specific metadata
│   │       └── metadata.go # XMPP IQ stanza metadata
│   ├── behavior/           # Behavior implementations
│   │   ├── behavior.go     # Behavior interface
│   │   ├── mqtt_declarative.go # MQTT declarative behavior
│   │   ├── registry.go     # Behavior registry and factory pattern
│   │   ├── usp.go          # USP/TR-369 behavior with boot event
│   │   └── xmpp_cwmp.go    # CWMP/TR-069 behavior with Inform sending
│   ├── client/             # Client interfaces
│   │   ├── client.go       # Generic Client interface
│   │   ├── mqtt/           # MQTT client implementation
│   │   │   └── client.go   # MQTT client wrapper
│   │   └── xmpp/           # XMPP client implementation
│   │       └── client.go   # XMPP client wrapper
│   ├── common/             # Common types
│   │   └── message.go      # Message interface
│   ├── config/             # Configuration handling
│   │   ├── config.go       # Config structs
│   │   └── template.go     # Template functions
│   ├── connector/          # Connection management
│   │   └── pool.go         # Connection pool
│   ├── cwmp/               # CWMP/TR-069 protocol definitions
│   │   ├── model.go        # CWMP SOAP model definitions
│   │   └── cwmp-msg.go     # CWMP Inform message builder
│   ├── engine/             # Core engine
│   │   └── engine.go       # Engine implementation
│   ├── generator/          # Credential generation
│   │   └── credential.go   # Client ID/username/password generation
│   ├── logging/            # Logging utilities
│   │   └── logging.go      # Logger implementation
│   ├── metrics/            # Prometheus metrics
│   │   └── metrics.go      # Metrics server
│   └── usp/                # USP/TR-369 protocol definitions
│       ├── usp-msg-1-5.proto      # USP Message protobuf schema
│       ├── usp-msg-1-5.pb.go      # Generated Go code
│       └── usp_record/            # USP Record protobuf
│           ├── usp-record-1-5.proto
│           └── usp-record-1-5.pb.go
└── examples/               # Example configurations
    ├── mqtt_publisher.yaml
    ├── mqtt_subscriber.yaml
    ├── device.yaml
    ├── usp-boot.yaml       # USP boot event example
    └── xmpp_cwmp_inform.yaml # CWMP Inform example
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Engine                                                          │
│ ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐ │
│ │ Worker Pool      │  │ Global Ticker    │  │ Rate Limiters   │ │
│ │ (10 Workers)     │  │ (1 sec interval) │  │ Connect/Sub     │ │
│ │ (channel per     │  │                  │  │ Send/Disconn    │ │
│ │   worker)        │  │                  │  │                 │ │
│ └────────┬─────────┘  └────────┬─────────┘  └────────┬────────┘ │
│          │                     │                     │          │
│ ┌────────▼─────────────────────▼─────────────────────▼────────┐ │
│ │ Action Queue (sharded by Client ID hash)                     │ │
│ └─────────────────────────────┬───────────────────────────────┘ │
└───────────────────────────────┼─────────────────────────────────┘
                                │ SubmitActions()
┌───────────────────────────────▼─────────────────────────────────┐
│ Client (per connection)                                         │
│ ┌──────────────────────────────────────────────────────────────┐ │
│ │ Client Interface                                             │ │
│ │ - Send / Subscribe / Unsubscribe / Disconnect               │ │
│ │ - IsConnected / ID                                           │ │
│ └──────────────────────────────────────────────────────────────┘ │
│ ┌──────────────────────────────────────────────────────────────┐ │
│ │ Protocol Adapter (e.g., paho.mqtt.golang)                   │ │
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
│ │ Returns []Action (Send/Subscribe/Unsubscribe/Disconnect)   │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Description |
|-----------|-------------|
| **Engine** | Core orchestrator that manages workers, rate limiting, and global timer |
| **Worker Pool** | 10 concurrent workers that dequeue and execute actions from sharded queues |
| **Action Queue** | Sharded by Client ID hash to ensure sequential execution per client |
| **Global Ticker** | Broadcasts tick events to all clients every 1 second |
| **Rate Limiters** | Token bucket rate limiters for connect, subscribe, send, and disconnect |
| **Client** | Protocol-agnostic client interface with adapter implementations |
| **Behavior** | Interface defining client behavior (OnConnect, OnMessage, OnTick, OnDisconnect) |
| **Action** | Protocol-agnostic action definitions (Send/Subscribe/Unsubscribe/Disconnect). Protocol-specific parameters are carried via `Metadata` map and interpreted by each client implementation. |

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
        Action goes to sharded queue (hash by Client ID)
            ↓
        Worker dequeues action
            ↓
        Engine.executeActions() → dispatches to specific handler
            ↓
        act.Execute(client) → Send/Subscribe etc.
            ↓
        Client.Send() / Client.Subscribe() etc.
            ↓
        Protocol adapter publishes/subscribes to broker
```

### Message-Driven Actions

```
Broker publishes message
        ↓
    Client receives message via protocol adapter
        ↓
    Behavior.OnMessage(client, msg) → returns []Action
        ↓
    Engine.SubmitActions(client, actions)
        ↓
    Action goes to sharded queue
        ↓
    Worker executes action (same as above)
```

## Protocol Support

| Protocol | Status | Notes |
|----------|--------|-------|
| MQTT | ✅ Supported | Using Eclipse Paho |
| XMPP | ✅ Supported | Using go-xmpp |
| WebSocket | 🔜 Planned | - |
