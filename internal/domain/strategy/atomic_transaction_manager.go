package strategy

import (
	"KVDB/internal/domain"
	"log"
	"sync"
)

type AtomicTransactionManager struct {
	subscribers         sync.Map // map[string]chan domain.TransactionResult
	currentTransactions sync.Map // map[string]domain.Transaction
	results             sync.Map // map[string]domain.TransactionResult

	currentInstance *domain.DbInstance

	conflictFinder         domain.ConflictDetector
	resolver               domain.ConflictResolver
	repository             domain.DbEntryRepository
	transactionBroadcaster domain.TransactionBroadcaster
}

func NewAtomicTransactionManager(im *domain.DbInstanceManager, repo domain.DbEntryRepository, tb domain.TransactionBroadcaster) *AtomicTransactionManager {
	a := &AtomicTransactionManager{
		conflictFinder:         &domain.ConflictFinder{},
		resolver:               &domain.FWWConflictResolver{},
		repository:             repo,
		transactionBroadcaster: tb,
	}
	go a.subscribeToCurrentInstance(im)
	return a
}

func (a *AtomicTransactionManager) subscribeToCurrentInstance(im *domain.DbInstanceManager) {
	ch := im.SubscribeToGetCurrentInstance()
	for msg := range ch {
		log.Println("TransactionManager: Setting current instance")
		a.currentInstance = &msg
	}
}

func (a *AtomicTransactionManager) Execute(t domain.Transaction) <-chan domain.TransactionResult {
	t.InstanceId = a.currentInstance.Id

	ch := make(chan domain.TransactionResult, 1)
	a.currentTransactions.Store(t.Id, t)
	a.subscribers.Store(t.Id, ch)

	// Copiamos transacciones actuales en un map para conflicto
	txs := make(map[string]domain.Transaction)
	a.currentTransactions.Range(func(key, value any) bool {
		txs[key.(string)] = value.(domain.Transaction)
		return true
	})
	conflict := a.conflictFinder.Check(txs, t)

	if conflict != nil {
		if _, found := conflict.Transactions()[t.Id]; !found {
			resolution := a.resolver.Resolve(*conflict)
			if _, aborting := resolution.AbortingTransactions[t.Id]; aborting {
				a.AbortTransaction(t.Id)
				return ch
			}
		}
	}

	result := a.execute(t)
	a.results.Store(t.Id, result)

	if err := a.transactionBroadcaster.BroadcastTransaction(t); err != nil {
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
		if sub, ok := a.subscribers.Load(t.Id); ok {
			if res, ok := a.results.Load(t.Id); ok {
				sub.(chan domain.TransactionResult) <- res.(domain.TransactionResult)
			}
			a.cleanTransaction(t)
		}
		return
	}

	a.currentTransactions.Store(t.Id, t)

	// Chequeamos conflicto con snapshot del mapa
	txs := make(map[string]domain.Transaction)
	a.currentTransactions.Range(func(key, value any) bool {
		txs[key.(string)] = value.(domain.Transaction)
		return true
	})
	conflict := a.conflictFinder.Check(txs, t)

	if conflict != nil {
		if _, found := conflict.Transactions()[t.Id]; !found {
			resolution := a.resolver.Resolve(*conflict)
			if _, aborting := resolution.AbortingTransactions[t.Id]; aborting {
				a.AbortTransaction(t.Id)
				return
			}
			for id := range resolution.AbortingTransactions {
				a.AbortTransaction(id)
			}
		}
	}

	a.execute(t)
	a.cleanTransaction(t)
}

func (a *AtomicTransactionManager) cleanTransaction(t domain.Transaction) {
	if t.InstanceId == a.currentInstance.Id {
		if sub, ok := a.subscribers.LoadAndDelete(t.Id); ok {
			close(sub.(chan domain.TransactionResult))
		}
	}
	a.currentTransactions.Delete(t.Id)
	a.results.Delete(t.Id)
}

func (a *AtomicTransactionManager) AbortTransaction(id string) {
	if val, ok := a.currentTransactions.Load(id); ok {
		t := val.(domain.Transaction)
		if t.InstanceId == a.currentInstance.Id {
			if sub, ok := a.subscribers.Load(id); ok {
				sub.(chan domain.TransactionResult) <- domain.TransactionResult{
					TransactionId: id,
					Success:       false,
				}
			}
		}
	}
	if sub, ok := a.subscribers.LoadAndDelete(id); ok {
		close(sub.(chan domain.TransactionResult))
	}
	a.currentTransactions.Delete(id)
	a.results.Delete(id)
}
