package domain

import (
	"github.com/google/uuid"
	"time"
)

type Transaction struct {
	Id         string
	ReadSet    map[string]DbEntry
	WriteSet   map[string]DbEntry
	DeleteSet  map[string]DbEntry
	Timestamp  int64
	InstanceId uint64
}

func NewTransaction() Transaction {
	return Transaction{
		Id:        uuid.NewString(),
		ReadSet:   make(map[string]DbEntry),
		WriteSet:  make(map[string]DbEntry),
		DeleteSet: make(map[string]DbEntry),
		Timestamp: time.Now().UnixNano(),
	}
}

func (t *Transaction) AddReadEntry(entry DbEntry) {
	t.ReadSet[entry.Key()] = entry.Copy()
}

func (t *Transaction) AddWriteEntry(entry DbEntry) {
	t.WriteSet[entry.Key()] = entry.Copy()
}

func (t *Transaction) AddDeleteEntry(entry DbEntry) {
	t.DeleteSet[entry.Key()] = entry.Copy()
}

func TransactionFromReadEntry(entry DbEntry) Transaction {
	transaction := NewTransaction()
	transaction.AddReadEntry(entry)
	return transaction
}

func (t *Transaction) ConflictsWith(other Transaction) bool {
	if t.IsEmpty() || other.IsEmpty() {
		return false
	}

	checkConflict := func(keys map[string]DbEntry) bool {
		for key := range keys {
			if _, exists := other.ReadSet[key]; exists {
				return true
			}
			if _, exists := other.WriteSet[key]; exists {
				return true
			}
			if _, exists := other.DeleteSet[key]; exists {
				return true
			}
		}
		return false
	}

	return checkConflict(t.WriteSet) || checkConflict(t.DeleteSet) || checkConflict(t.ReadSet)
}

func (t *Transaction) IsEmpty() bool {
	return len(t.WriteSet) == 0 && len(t.DeleteSet) == 0
}

func (t *Transaction) IsReadOnly() bool {
	return len(t.WriteSet) == 0 && len(t.DeleteSet) == 0 && len(t.ReadSet) > 0
}

func (t *Transaction) IsWriteOrDelete() bool {
	return len(t.DeleteSet) > 0 || len(t.WriteSet) > 0
}

func TransactionFromWriteEntry(entry DbEntry) Transaction {
	transaction := NewTransaction()
	transaction.AddWriteEntry(entry)
	return transaction
}

func TransactionFromDeleteEntry(entry DbEntry) Transaction {
	transaction := NewTransaction()
	transaction.AddDeleteEntry(entry)
	return transaction
}
