package strategy

import (
	"KVDB/internal/domain"
	"sync"
)

type EventualTransactionManager struct {
	currentTransactions map[string]domain.Transaction
	repository          domain.DbEntryRepository
	broadcaster         domain.TransactionBroadcaster
	mu                  sync.Mutex
}

func NewEventualTransactionManager(repository domain.DbEntryRepository,
	broadcaster domain.TransactionBroadcaster) *EventualTransactionManager {
	return &EventualTransactionManager{
		currentTransactions: make(map[string]domain.Transaction),
		repository:          repository,
		broadcaster:         broadcaster,
	}
}

func (e *EventualTransactionManager) Execute(transaction domain.Transaction) <-chan domain.TransactionResult {
	ch := make(chan domain.TransactionResult, 1)

	e.mu.Lock()
	ch <- e.execute(transaction)
	e.mu.Unlock()

	go func() {
		err := e.broadcaster.BroadcastTransaction(transaction)

		if err != nil {
			ch <- domain.FromTransaction(transaction)
		}
	}()
	go close(ch)

	return ch
}

func (e *EventualTransactionManager) execute(transaction domain.Transaction) domain.TransactionResult {
	for _, entry := range transaction.WriteSet {
		e.repository.Save(entry)
	}
	for _, entry := range transaction.DeleteSet {
		e.repository.Delete(entry.Key())
	}
	result := domain.FromTransaction(transaction)
	result.MarkAsSuccessful()
	return result
}

func (e *EventualTransactionManager) AddTransaction(transaction domain.Transaction) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.execute(transaction)
}

func (e *EventualTransactionManager) AbortTransaction(id string) {
	//TODO implement me
	panic("implement me")
}
