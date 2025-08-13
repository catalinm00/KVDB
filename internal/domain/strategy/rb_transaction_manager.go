package strategy

import (
	"KVDB/internal/domain"
	"sync"
)

type RbTransactionManager struct {
	subscribers            map[string]chan domain.TransactionResult
	currentInstance        *domain.DbInstance
	instanceManager        *domain.DbInstanceManager
	CurrentTransactions    map[string]domain.Transaction
	transactionBroadcaster domain.TransactionBroadcaster
	commitAckManager       domain.CommitAckManager
	commitAckSender        domain.CommitAckSender
	conflictDetector       domain.ConflictDetector
	conflictResolver       domain.ConflictResolver
	dbEntryRepository      domain.DbEntryRepository
	mu                     sync.RWMutex
}

func NewRbTransactionManager(tb domain.TransactionBroadcaster, cam *domain.TransactionCommitAckManager,
	repository domain.DbEntryRepository, ackSender domain.CommitAckSender, im *domain.DbInstanceManager) *RbTransactionManager {
	tm := &RbTransactionManager{
		CurrentTransactions:    make(map[string]domain.Transaction),
		transactionBroadcaster: tb,
		commitAckManager:       cam,
		commitAckSender:        ackSender,
		conflictDetector:       &domain.ConflictFinder{},
		conflictResolver:       &domain.LWWConflictResolver{},
		dbEntryRepository:      repository,
		instanceManager:        im,
		subscribers:            make(map[string]chan domain.TransactionResult),
	}
	tm.setCurrentInstance()
	return tm
}

func (tm *RbTransactionManager) setCurrentInstance() {
	go func() {
		resCh := tm.instanceManager.SubscribeToGetCurrentInstance()
		res := <-resCh
		tm.currentInstance = &res
	}()
}

func (tm *RbTransactionManager) Execute(t domain.Transaction) <-chan domain.TransactionResult {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	t.InstanceId = tm.currentInstance.Id
	tm.CurrentTransactions[t.Id] = t
	ch := make(chan domain.TransactionResult, 1)
	tm.subscribers[t.Id] = ch
	err := tm.transactionBroadcaster.BroadcastTransaction(t)
	if err != nil {
		ch <- domain.FromTransaction(t)
		return ch
	}

	err = tm.transactionBroadcaster.BroadcastCommitInit(t)
	if err != nil {
		ch <- domain.FromTransaction(t)
		return ch
	}
	return ch
}

func (tm *RbTransactionManager) StartTransaction(transaction domain.Transaction) {
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

func (tm *RbTransactionManager) AddTransaction(transaction domain.Transaction) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	transaction.InstanceId = tm.currentInstance.Id
	tm.CurrentTransactions[transaction.Id] = transaction
}

func (tm *RbTransactionManager) StartTransactionAbortion(transaction domain.Transaction) {
	tm.subscribers[transaction.Id] <- domain.FromTransaction(transaction)
	err := tm.transactionBroadcaster.BroadcastAbort(transaction)
	if err != nil {
		return
	}
}

func (tm *RbTransactionManager) AbortTransaction(transactionId string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	_, exists := tm.CurrentTransactions[transactionId]
	if !exists {
		return
	}
	delete(tm.CurrentTransactions, transactionId)
	tm.commitAckManager.Remove(transactionId)
}

func (tm *RbTransactionManager) StartCommitInit(transaction domain.Transaction) {
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

func (tm *RbTransactionManager) InitCommit(transaction domain.Transaction) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if _, exists := tm.CurrentTransactions[transaction.Id]; !exists {
		return
	}
	conflict := tm.conflictDetector.Check(tm.CurrentTransactions, transaction)
	if conflict != nil {
		resolution := tm.conflictResolver.Resolve(*conflict)
		for _, abortingTransaction := range resolution.AbortingTransactions {
			ack := domain.NewTransactionCommitAck(abortingTransaction.Id, tm.currentInstance.Id, transaction.InstanceId, false)
			tm.commitAckSender.SendCommitAck(ack)
		}

		for _, committingTransaction := range resolution.CommitingTransactions {
			ack := domain.NewTransactionCommitAck(committingTransaction.Id, tm.currentInstance.Id, transaction.InstanceId, true)
			tm.commitAckSender.SendCommitAck(ack)

		}
		return
	}

	ack := domain.NewTransactionCommitAck(transaction.Id, tm.currentInstance.Id, transaction.InstanceId, true)
	err := tm.commitAckSender.SendCommitAck(ack)
	if err != nil {
		return
	}
}

func (tm *RbTransactionManager) StartCommitConfirmation(transaction domain.Transaction) {
	if _, exists := tm.CurrentTransactions[transaction.Id]; !exists {
		return
	}
	err := tm.transactionBroadcaster.BroadcastCommitConfirmation(transaction)
	if err != nil {
		return
	}
}

func (tm *RbTransactionManager) ConfirmCommit(transaction domain.Transaction) {
	for _, entry := range transaction.WriteSet {
		tm.dbEntryRepository.Save(entry)
	}
	for _, entry := range transaction.DeleteSet {
		tm.dbEntryRepository.Delete(entry.Key())
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.CurrentTransactions, transaction.Id)
	tm.commitAckManager.Remove(transaction.Id)
	if transaction.InstanceId == tm.currentInstance.Id {
		result := domain.FromTransaction(transaction)
		result.MarkAsSuccessful()
		tm.subscribers[transaction.Id] <- result
	}
}

func (tm *RbTransactionManager) AddCommitAck(ack domain.TransactionCommitAck) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
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
