package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLWWConflictResolver_Resolve(t *testing.T) {
	resolver := &LWWConflictResolver{}

	// Crear transacciones con timestamps diferentes
	txOld := NewTransaction()
	txOld.Timestamp = time.Now().Add(-time.Minute).UnixNano()
	txOld.AddWriteEntry(NewDbEntry("k", "v1", false))
	txNew := NewTransaction()
	txNew.AddWriteEntry(NewDbEntry("k", "v2", false))
	txNew.Timestamp = time.Now().UnixNano()

	conflict := NewConflict()
	conflict.AddTransaction(txOld)
	conflict.AddTransaction(txNew)

	resolution := resolver.Resolve(*conflict)

	// Solo la transacción más reciente debe estar en CommitingTransactions
	assert.Len(t, resolution.CommitingTransactions, 1)
	assert.Contains(t, resolution.CommitingTransactions, txNew.Id)
	assert.Equal(t, txNew.Id, resolution.CommitingTransactions[txNew.Id].Id)

	// La otra debe estar en AbortingTransactions
	assert.Len(t, resolution.AbortingTransactions, 1)
	assert.Contains(t, resolution.AbortingTransactions, txOld.Id)
}
