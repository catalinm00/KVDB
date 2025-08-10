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

func (m *DbInstanceManager) GetById(id uint64) *DbInstance {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.CurrentInstance != nil && m.CurrentInstance.Id == id {
		return m.CurrentInstance
	}
	if m.Replicas != nil {
		for _, replica := range *m.Replicas {
			if replica.Id == id {
				return &replica
			}
		}
	}
	return nil
}
