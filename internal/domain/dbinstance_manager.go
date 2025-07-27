package domain

import "sync"

type DbInstanceManager struct {
	CurrentInstance *DbInstance
	Replicas        *[]DbInstance
	mu              sync.RWMutex
}

func NewDbInstanceManager() *DbInstanceManager {
	return &DbInstanceManager{}
}

func (m *DbInstanceManager) SetCurrentInstance(instance *DbInstance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CurrentInstance = instance
}

func (m *DbInstanceManager) SetReplicas(replicas *[]DbInstance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Replicas = replicas
}
