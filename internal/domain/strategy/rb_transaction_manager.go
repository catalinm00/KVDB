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
	conflictDetector       domain.ConflictDetector
	conflictResolver       domain.ConflictResolver
	dbEntryRepository      domain.DbEntryRepository
	mu                     sync.RWMutex
}

func NewRbTransactionManager(tb domain.TransactionBroadcaster, cam *domain.TransactionCommitAckManager,
	repository domain.DbEntryRepository, im *domain.DbInstanceManager) *RbTransactionManager {
	tm := &RbTransactionManager{
		CurrentTransactions:    make(map[string]domain.Transaction),
		transactionBroadcaster: tb,
		commitAckManager:       cam,
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
	t.InstanceId = tm.currentInstance.Id
	tm.CurrentTransactions[t.Id] = t
	ch := make(chan domain.TransactionResult, 1)
	tm.subscribers[t.Id] = ch
	tm.mu.Unlock()

	err := tm.transactionBroadcaster.BroadcastTransaction(t)
	if err != nil {
		ch <- domain.FromTransaction(t)
		return ch
	}

	tm.StartInitCommit(t)

	return ch
}

func (tm *RbTransactionManager) AddTransaction(transaction domain.Transaction) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	transaction.InstanceId = tm.currentInstance.Id
	tm.CurrentTransactions[transaction.Id] = transaction
}

func (tm *RbTransactionManager) StartTransactionAbortion(transaction domain.Transaction) {
	var ch chan domain.TransactionResult
	tm.mu.Lock()
	ch = tm.subscribers[transaction.Id]
	delete(tm.subscribers, transaction.Id)
	tm.mu.Unlock()

	ch <- domain.FromTransaction(transaction)
	close(ch)

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

func (tm *RbTransactionManager) InitCommit(transaction domain.Transaction) {
	tm.mu.RLock()
	if _, exists := tm.CurrentTransactions[transaction.Id]; !exists {
		return
	}
	conflict := tm.conflictDetector.Check(tm.CurrentTransactions, transaction)
	tm.mu.RUnlock()

	if conflict != nil {
		resolution := tm.conflictResolver.Resolve(*conflict)
		for _, abortingTransaction := range resolution.AbortingTransactions {
			ack := domain.NewTransactionCommitAck(abortingTransaction.Id, tm.currentInstance.Id, transaction.InstanceId, false)
			tm.AddCommitAck(ack)
			tm.transactionBroadcaster.BroadcastAck(ack)
		}

		for _, committingTransaction := range resolution.CommitingTransactions {
			ack := domain.NewTransactionCommitAck(committingTransaction.Id, tm.currentInstance.Id, transaction.InstanceId, true)
			tm.AddCommitAck(ack)
			tm.transactionBroadcaster.BroadcastAck(ack)
		}
		return
	}

	ack := domain.NewTransactionCommitAck(transaction.Id, tm.currentInstance.Id, transaction.InstanceId, true)
	tm.AddCommitAck(ack)
	err := tm.transactionBroadcaster.BroadcastAck(ack)
	if err != nil {
		return
	}
}

func (tm *RbTransactionManager) StartInitCommit(transaction domain.Transaction) {
	tm.mu.RLock()
	if _, exists := tm.CurrentTransactions[transaction.Id]; !exists {
		tm.mu.RUnlock()
		return
	}

	conflict := tm.conflictDetector.Check(tm.CurrentTransactions, transaction)
	tm.mu.RUnlock()

	if conflict != nil {
		resolution := tm.conflictResolver.Resolve(*conflict)
		for _, abortingTransaction := range resolution.AbortingTransactions {
			ack := domain.NewTransactionCommitAck(abortingTransaction.Id, tm.currentInstance.Id, transaction.InstanceId, false)
			tm.commitAckManager.Add(ack)
			tm.transactionBroadcaster.BroadcastAck(ack)
		}

		for _, committingTransaction := range resolution.CommitingTransactions {
			ack := domain.NewTransactionCommitAck(committingTransaction.Id, tm.currentInstance.Id, transaction.InstanceId, true)
			tm.commitAckManager.Add(ack)
			tm.transactionBroadcaster.BroadcastAck(ack)
		}
		return
	}
	err := tm.transactionBroadcaster.BroadcastCommitInit(transaction)
	if err != nil {
		return
	}

	ack := domain.NewTransactionCommitAck(transaction.Id, tm.currentInstance.Id, transaction.InstanceId, true)
	tm.commitAckManager.Add(ack)
	err = tm.transactionBroadcaster.BroadcastAck(ack)
	if err != nil {
		return
	}
}

func (tm *RbTransactionManager) ConfirmCommit(transaction domain.Transaction) {
	var ch chan domain.TransactionResult
	tm.mu.Lock()
	delete(tm.CurrentTransactions, transaction.Id)
	tm.commitAckManager.Remove(transaction.Id)
	if transaction.InstanceId == tm.currentInstance.Id {
		ch = tm.subscribers[transaction.Id]
		delete(tm.subscribers, transaction.Id)
	}
	tm.mu.Unlock()

	for _, entry := range transaction.WriteSet {
		tm.dbEntryRepository.Save(entry)
	}
	for _, entry := range transaction.DeleteSet {
		tm.dbEntryRepository.Delete(entry.Key())
	}
	if ch != nil {
		result := domain.FromTransaction(transaction)
		result.MarkAsSuccessful()
		ch <- result
		close(ch)
	}
}

func (tm *RbTransactionManager) AddCommitAck(ack domain.TransactionCommitAck) {
	tm.mu.RLock()
	transaction, exists := tm.CurrentTransactions[ack.TransactionId]
	tm.mu.RUnlock()
	if !exists {
		return
	}

	if !ack.Valid {
		tm.StartTransactionAbortion(transaction)
		tm.AbortTransaction(transaction.Id)
		return
	}
	tm.commitAckManager.Add(ack)
	if !tm.commitAckManager.AckedByAllInstances(ack.TransactionId) ||
		!tm.commitAckManager.HasOnlyPositiveAcks(ack.TransactionId) {
		return
	}

	tm.ConfirmCommit(transaction)
}
