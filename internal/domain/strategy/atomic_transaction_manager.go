package strategy

import (
	"KVDB/internal/domain"
	"log"
	"sync"
)

type AtomicTransactionManager struct {
	subscribers            map[string]chan domain.TransactionResult
	currentTransactions    map[string]domain.Transaction
	results                map[string]domain.TransactionResult
	currentInstance        *domain.DbInstance
	instanceManager        *domain.DbInstanceManager
	conflictFinder         domain.ConflictDetector
	resolver               domain.ConflictResolver
	repository             domain.DbEntryRepository
	transactionBroadcaster domain.TransactionBroadcaster
	mu                     sync.RWMutex
}

func NewAtomicTransactionManager(im *domain.DbInstanceManager, repo domain.DbEntryRepository, tb domain.TransactionBroadcaster) *AtomicTransactionManager {
	a := &AtomicTransactionManager{
		subscribers:            make(map[string]chan domain.TransactionResult, 20000),
		currentTransactions:    make(map[string]domain.Transaction, 20000),
		results:                make(map[string]domain.TransactionResult, 20000),
		instanceManager:        im,
		conflictFinder:         &domain.ConflictFinder{},
		resolver:               &domain.FWWConflictResolver{},
		repository:             repo,
		transactionBroadcaster: tb,
	}
	go a.subscribeToCurrentInstance()
	return a
}

func (a *AtomicTransactionManager) subscribeToCurrentInstance() {
	ch := a.instanceManager.SubscribeToGetCurrentInstance()
	for msg := range ch {
		a.mu.Lock()
		log.Println("TransactionManager: Setting current instance")
		a.currentInstance = &msg
		a.mu.Unlock()
	}
}

func (a *AtomicTransactionManager) setCurrentInstance() {
	go func() {
		resCh := a.instanceManager.SubscribeToGetCurrentInstance()
		res := <-resCh
		a.currentInstance = &res
	}()
}

func (a *AtomicTransactionManager) Execute(t domain.Transaction) <-chan domain.TransactionResult {
	a.mu.Lock()
	t.InstanceId = a.currentInstance.Id
	a.currentTransactions[t.Id] = t
	ch := make(chan domain.TransactionResult, 1)
	a.subscribers[t.Id] = ch
	a.mu.Unlock()

	a.mu.RLock()
	conflict := a.conflictFinder.Check(a.currentTransactions, t)
	a.mu.RUnlock()

	if conflict != nil {
		_, found := conflict.Transactions()[t.Id]
		if !found {
			resolution := a.resolver.Resolve(*conflict)
			if _, found := resolution.AbortingTransactions[t.Id]; found {
				a.AbortTransaction(t.Id)
				return ch
			}
		}
	}
	a.mu.Lock()
	a.results[t.Id] = a.execute(t)
	a.mu.Unlock()

	err := a.transactionBroadcaster.BroadcastTransaction(t)
	if err != nil {
		ch <- domain.FromTransaction(t)
		return ch
	}

	return ch
}

func (a *AtomicTransactionManager) execute(transaction domain.Transaction) domain.TransactionResult {
	for _, entry := range transaction.WriteSet {
		a.repository.Save(entry)
	}
	for _, entry := range transaction.DeleteSet {
		a.repository.Delete(entry.Key())
	}
	result := domain.FromTransaction(transaction)
	result.MarkAsSuccessful()
	return result
}

func (a *AtomicTransactionManager) AddTransaction(t domain.Transaction) {

	if t.InstanceId == a.currentInstance.Id {
		a.mu.Lock()
		a.subscribers[t.Id] <- a.results[t.Id]
		a.cleanTransaction(t)
		a.mu.Unlock()
		return
	}

	a.mu.Lock()
	a.currentTransactions[t.Id] = t
	conflict := a.conflictFinder.Check(a.currentTransactions, t)
	a.mu.Unlock()

	if conflict != nil {
		_, found := conflict.Transactions()[t.Id]
		if !found {
			resolution := a.resolver.Resolve(*conflict)
			if _, found := resolution.AbortingTransactions[t.Id]; found {
				a.AbortTransaction(t.Id)
				return
			}
			for id, _ := range resolution.AbortingTransactions {
				a.AbortTransaction(id)
			}
		}
	}

	a.mu.Lock()
	a.execute(t)
	a.cleanTransaction(t)
	a.mu.Unlock()
}

func (a *AtomicTransactionManager) cleanTransaction(t domain.Transaction) {
	if t.InstanceId == a.currentInstance.Id {
		close(a.subscribers[t.Id])
		delete(a.subscribers, t.Id)
	}
	delete(a.currentTransactions, t.Id)
	delete(a.results, t.Id)
}

func (a *AtomicTransactionManager) AbortTransaction(id string) {
	a.mu.Lock()
	if a.currentTransactions[id].InstanceId == a.currentInstance.Id {
		a.subscribers[id] <- domain.TransactionResult{TransactionId: id, Success: false}
	}
	close(a.subscribers[id])
	delete(a.subscribers, id)
	delete(a.currentTransactions, id)
	delete(a.results, id)
	a.mu.Unlock()
}
