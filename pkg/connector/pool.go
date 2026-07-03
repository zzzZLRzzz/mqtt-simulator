package connector

import (
	"sync"
)

type ConnectionPool struct {
	mu          sync.RWMutex
	connections map[string]*MQTTClient
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]*MQTTClient),
	}
}

func (p *ConnectionPool) Add(client *MQTTClient) {
	p.mu.Lock()
	p.connections[client.ClientID()] = client
	p.mu.Unlock()
}

func (p *ConnectionPool) Remove(clientID string) {
	p.mu.Lock()
	delete(p.connections, clientID)
	p.mu.Unlock()
}

func (p *ConnectionPool) Get(clientID string) (*MQTTClient, bool) {
	p.mu.RLock()
	client, ok := p.connections[clientID]
	p.mu.RUnlock()
	return client, ok
}

func (p *ConnectionPool) All() []*MQTTClient {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*MQTTClient, 0, len(p.connections))
	for _, c := range p.connections {
		result = append(result, c)
	}
	return result
}

func (p *ConnectionPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.connections)
}

func (p *ConnectionPool) StopAllClients() {
	clients := p.All()
	for _, client := range clients {
		client.StopReceiving()
	}
}

func (p *ConnectionPool) DisconnectAll() {
	clients := p.All()
	for _, client := range clients {
		_ = client.Disconnect()
	}
	p.mu.Lock()
	p.connections = make(map[string]*MQTTClient)
	p.mu.Unlock()
}
