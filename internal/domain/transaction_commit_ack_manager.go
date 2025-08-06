package domain

import (
	"sync"
	"time"
)

type CommitAckManager interface {
	AckedByAllInstances(transactionId string) bool
	HasOnlyPositiveAcks(transactionId string) bool
	Add(commitAck TransactionCommitAck)
	Remove(transactionId string)
}

type TransactionCommitAckManager struct {
	commitAckHolders map[string]*TransactionCommitAckHolder
	instanceManager  *DbInstanceManager
	mu               sync.RWMutex
}

func NewTransactionCommitAckManager(im *DbInstanceManager) *TransactionCommitAckManager {
	return &TransactionCommitAckManager{
		commitAckHolders: make(map[string]*TransactionCommitAckHolder),
		instanceManager:  im,
	}
}

func (t *TransactionCommitAckManager) AckedByAllInstances(transactionId string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	holder, exists := t.commitAckHolders[transactionId]
	if !exists {
		return false
	}

	ackCount := holder.CountAcks()
	instanceCount := 1
	if t.instanceManager.Replicas != nil {
		instanceCount = len(*t.instanceManager.Replicas)
	}

	return ackCount >= instanceCount
}

func (t *TransactionCommitAckManager) HasOnlyPositiveAcks(transactionId string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	holder, exists := t.commitAckHolders[transactionId]
	if !exists {
		return false
	}

	for _, ack := range holder.Acks() {
		if !ack.Valid {
			return false
		}
	}
	return true
}

func (t *TransactionCommitAckManager) Add(commitAck TransactionCommitAck) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.commitAckHolders[commitAck.TransactionId]; !exists {
		t.commitAckHolders[commitAck.TransactionId] = NewTransactionCommitAckHolder()
	}
	holder := t.commitAckHolders[commitAck.TransactionId]
	holder.Add(commitAck)
}

func (t *TransactionCommitAckManager) Remove(transactionId string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.commitAckHolders, transactionId)
}

type TransactionCommitAckHolder struct {
	acks []TransactionCommitAck
	mu   sync.RWMutex
}

func NewTransactionCommitAckHolder() *TransactionCommitAckHolder {
	return &TransactionCommitAckHolder{
		acks: make([]TransactionCommitAck, 0),
	}
}

type TransactionCommitAck struct {
	TransactionId      string `json:"transaction_id,omitempty"`
	SenderInstanceId   uint64 `json:"sender_instance_id,omitempty"`
	ReceiverInstanceId uint64 `json:"receiver_instance_id,omitempty"`
	Timestamp          int64  `json:"timestamp,omitempty"`
	Valid              bool   `json:"valid,omitempty"`
}

func NewTransactionCommitAck(transactionId string, senderInstanceId uint64, receiverInstanceId uint64, valid bool) TransactionCommitAck {
	return TransactionCommitAck{
		TransactionId:      transactionId,
		SenderInstanceId:   senderInstanceId,
		ReceiverInstanceId: receiverInstanceId,
		Timestamp:          time.Now().UnixNano(),
		Valid:              valid,
	}
}

func (t *TransactionCommitAckHolder) Add(ack TransactionCommitAck) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.acks = append(t.acks, ack)
}

func (t *TransactionCommitAckHolder) Acks() []TransactionCommitAck {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.acks
}

func (t *TransactionCommitAckHolder) CountAcks() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.acks)
}
