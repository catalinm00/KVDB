package domain

import "sync"

type DbInstanceManager struct {
	CurrentInstance *DbInstance
	Replicas        *[]DbInstance
	mu              sync.RWMutex
	subscribers     []chan []DbInstance
	ciSubscribers   []chan DbInstance
}

func NewDbInstanceManager() *DbInstanceManager {
	return &DbInstanceManager{
		subscribers: []chan []DbInstance{},
	}
}

func (m *DbInstanceManager) SetCurrentInstance(instance *DbInstance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CurrentInstance = instance
	for _, ch := range m.ciSubscribers {
		ch <- *instance
	}
}

func (m *DbInstanceManager) SetReplicas(replicas *[]DbInstance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.updateSubscribers()
	m.Replicas = replicas
}

func (m *DbInstanceManager) updateSubscribers() {
	for _, ch := range m.subscribers {
		go func(c chan []DbInstance) {
			c <- *m.Replicas
		}(ch)
	}
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

func (m *DbInstanceManager) Subscribe() <-chan []DbInstance {
	ch := make(chan []DbInstance)
	m.subscribers = append(m.subscribers, ch)
	return ch
}

func (m *DbInstanceManager) SubscribeToGetCurrentInstance() <-chan DbInstance {
	ch := make(chan DbInstance)
	m.ciSubscribers = append(m.ciSubscribers, ch)
	return ch
}
