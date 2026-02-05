package transport

import "sync"

type ConnectionPool struct {
	connections map[string]*connection
	mu          sync.RWMutex
}

func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]*connection),
	}
}

func (p *ConnectionPool) Add(conn *connection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connections[conn.connectionID] = conn
}

func (p *ConnectionPool) Get(connectionID string) *connection {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connections[connectionID]
}

func (p *ConnectionPool) Close(connectionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.connections[connectionID]; ok {
		close(conn.send)
		delete(p.connections, connectionID)
	}
}

func (p *ConnectionPool) Has(connectionID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.connections[connectionID]
	return ok
}

func (p *ConnectionPool) All() []*connection {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*connection, 0, len(p.connections))
	for _, conn := range p.connections {
		result = append(result, conn)
	}
	return result
}

func (p *ConnectionPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.connections)
}
