package domain

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConflictFinder_NoConflictWithDifferentKeys(t *testing.T) {
	cf := &ConflictFinder{}

	tx1 := NewTransaction()
	tx2 := NewTransaction()
	tx1.AddWriteEntry(NewDbEntry("a", "1", false))
	tx2.AddReadEntry(NewDbEntry("b", "2", false))

	current := map[string]Transaction{
		tx1.Id: tx1,
	}

	conflict := cf.Check(current, tx2)
	assert.Nil(t, conflict, "No debe haber conflicto si las claves no coinciden")
}

func TestConflictFinder_DetectsConflict(t *testing.T) {
	cf := &ConflictFinder{}

	tx1 := NewTransaction()
	tx2 := NewTransaction()
	tx1.AddWriteEntry(NewDbEntry("k", "v2", false))
	tx2.AddWriteEntry(NewDbEntry("k", "v3", false))

	current := map[string]Transaction{
		tx1.Id: tx1,
	}

	conflict := cf.Check(current, tx2)
	assert.NotNil(t, conflict, "Debe detectar conflicto")
	assert.Contains(t, conflict.Transactions(), tx1.Id)
	assert.Contains(t, conflict.Transactions(), tx2.Id)
}

func TestConflictFinder_NoConflictWithSelf(t *testing.T) {
	cf := &ConflictFinder{}

	tx := NewTransaction()
	tx.AddWriteEntry(NewDbEntry("k", "v", false))

	current := map[string]Transaction{
		tx.Id: tx,
	}

	conflict := cf.Check(current, tx)
	assert.Nil(t, conflict, "No debe haber conflicto consigo misma")
}
