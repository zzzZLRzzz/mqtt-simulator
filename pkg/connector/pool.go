package connector

import (
	"sync"

	"conn-conductor/pkg/client"
)

type ConnectionPool struct {
	mu          sync.RWMutex
	connections map[string]client.Client
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]client.Client),
	}
}

func (p *ConnectionPool) Add(client client.Client) {
	p.mu.Lock()
	p.connections[client.ID()] = client
	p.mu.Unlock()
}

func (p *ConnectionPool) Remove(clientID string) {
	p.mu.Lock()
	delete(p.connections, clientID)
	p.mu.Unlock()
}

func (p *ConnectionPool) Get(clientID string) (client.Client, bool) {
	p.mu.RLock()
	client, ok := p.connections[clientID]
	p.mu.RUnlock()
	return client, ok
}

func (p *ConnectionPool) All() []client.Client {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]client.Client, 0, len(p.connections))
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
	p.connections = make(map[string]client.Client)
	p.mu.Unlock()
}
