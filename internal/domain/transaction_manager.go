package domain

import (
	"sync"
)

type TransactionExecutionStrategy interface {
	Execute(t Transaction) <-chan TransactionResult
	AddTransaction(transaction Transaction)
	AbortTransaction(id string)
}

type BasicTransactionManager interface {
	AddTransaction(transaction Transaction)
	AbortTransaction(id string)
}

type ReliableBroadcastTransactionManager interface {
	InitCommit(t Transaction)
	ConfirmCommit(t Transaction)
	AddCommitAck(ack TransactionCommitAck)
}
type TransactionManager struct {
	subscribers            map[string]chan TransactionResult
	currentInstance        *DbInstance
	instanceManager        *DbInstanceManager
	CurrentTransactions    map[string]Transaction
	transactionBroadcaster TransactionBroadcaster
	commitAckManager       CommitAckManager
	commitAckSender        CommitAckSender
	conflictDetector       ConflictDetector
	conflictResolver       ConflictResolver
	dbEntryRepository      DbEntryRepository
	mu                     sync.RWMutex
}

type TransactionBroadcaster interface {
	BroadcastTransaction(transaction Transaction) error
	BroadcastAbort(transaction Transaction) error
	BroadcastCommitInit(transaction Transaction) error
	BroadcastCommitConfirmation(transaction Transaction) error
}

type CommitAckSender interface {
	SendCommitAck(ack TransactionCommitAck) error
}

func NewTransactionManager(tb TransactionBroadcaster, cam *TransactionCommitAckManager,
	repository DbEntryRepository, ackSender CommitAckSender, im *DbInstanceManager) *TransactionManager {
	tm := &TransactionManager{
		CurrentTransactions:    make(map[string]Transaction),
		transactionBroadcaster: tb,
		commitAckManager:       cam,
		commitAckSender:        ackSender,
		conflictDetector:       &ConflictFinder{},
		conflictResolver:       &LWWConflictResolver{},
		dbEntryRepository:      repository,
		instanceManager:        im,
		subscribers:            make(map[string]chan TransactionResult),
	}
	tm.setCurrentInstance()
	return tm
}

func (tm *TransactionManager) setCurrentInstance() {
	go func() {
		resCh := tm.instanceManager.SubscribeToGetCurrentInstance()
		res := <-resCh
		tm.currentInstance = &res
	}()
}

func (tm *TransactionManager) Execute(t Transaction) <-chan TransactionResult {
	t.InstanceId = tm.currentInstance.Id
	tm.CurrentTransactions[t.Id] = t
	ch := make(chan TransactionResult, 1)
	tm.subscribers[t.Id] = ch
	err := tm.transactionBroadcaster.BroadcastTransaction(t)
	if err != nil {
		ch <- FromTransaction(t)
		return ch
	}

	err = tm.transactionBroadcaster.BroadcastCommitInit(t)
	if err != nil {
		ch <- FromTransaction(t)
		return ch
	}
	return ch
}

func (tm *TransactionManager) StartTransaction(transaction Transaction) {
	tm.AddTransaction(transaction)

	err := tm.transactionBroadcaster.BroadcastTransaction(transaction)
	if err != nil {
		return
	}

	err = tm.transactionBroadcaster.BroadcastCommitInit(transaction)
	if err != nil {
		return
	}
}

func (tm *TransactionManager) AddTransaction(transaction Transaction) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	transaction.InstanceId = tm.currentInstance.Id
	tm.CurrentTransactions[transaction.Id] = transaction
}

func (tm *TransactionManager) StartTransactionAbortion(transaction Transaction) {
	tm.subscribers[transaction.Id] <- FromTransaction(transaction)
	err := tm.transactionBroadcaster.BroadcastAbort(transaction)
	if err != nil {
		return
	}
}

func (tm *TransactionManager) AbortTransaction(transactionId string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	_, exists := tm.CurrentTransactions[transactionId]
	if !exists {
		return
	}
	delete(tm.CurrentTransactions, transactionId)
	tm.commitAckManager.Remove(transactionId)
}

func (tm *TransactionManager) StartCommitInit(transaction Transaction) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if _, exists := tm.CurrentTransactions[transaction.Id]; !exists {
		return
	}
	err := tm.transactionBroadcaster.BroadcastCommitInit(transaction)
	if err != nil {
		return
	}
}

func (tm *TransactionManager) InitCommit(transaction Transaction) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if _, exists := tm.CurrentTransactions[transaction.Id]; !exists {
		return
	}
	conflict := tm.conflictDetector.Check(tm.CurrentTransactions, transaction)
	resolution := tm.conflictResolver.Resolve(*conflict)
	for _, abortingTransaction := range resolution.AbortingTransactions {
		ack := NewTransactionCommitAck(abortingTransaction.Id, tm.currentInstance.Id, transaction.InstanceId, false)
		tm.commitAckSender.SendCommitAck(ack)
	}

	for _, committingTransaction := range resolution.CommitingTransactions {
		ack := NewTransactionCommitAck(committingTransaction.Id, tm.currentInstance.Id, transaction.InstanceId, true)
		tm.commitAckSender.SendCommitAck(ack)
	}
}

func (tm *TransactionManager) StartCommitConfirmation(transaction Transaction) {
	if _, exists := tm.CurrentTransactions[transaction.Id]; !exists {
		return
	}
	err := tm.transactionBroadcaster.BroadcastCommitConfirmation(transaction)
	if err != nil {
		return
	}
}

func (tm *TransactionManager) ConfirmCommit(transaction Transaction) {
	for _, entry := range transaction.WriteSet {
		tm.dbEntryRepository.Save(entry)
	}
	for _, entry := range transaction.DeleteSet {
		tm.dbEntryRepository.Delete(entry.key)
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.CurrentTransactions, transaction.Id)
	tm.commitAckManager.Remove(transaction.Id)
	if transaction.InstanceId == tm.currentInstance.Id {
		result := FromTransaction(transaction)
		result.MarkAsSuccessful()
		tm.subscribers[transaction.Id] <- result
	}
}

func (tm *TransactionManager) AddCommitAck(ack TransactionCommitAck) {
	if !ack.Valid {
		transaction, exists := tm.CurrentTransactions[ack.TransactionId]
		if !exists {
			return
		}

		tm.StartTransactionAbortion(transaction)
		return
	}
	tm.commitAckManager.Add(ack)
	if !tm.commitAckManager.AckedByAllInstances(ack.TransactionId) {
		return
	}

	transaction, exists := tm.CurrentTransactions[ack.TransactionId]
	if !exists {
		return
	}
	tm.StartCommitConfirmation(transaction)
}
