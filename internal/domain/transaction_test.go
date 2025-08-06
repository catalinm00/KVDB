package domain

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAddReadWriteDeleteEntry(t *testing.T) {
	tx := NewTransaction()
	entry := NewDbEntry("k1", "v1", false)

	tx.AddReadEntry(entry)
	tx.AddWriteEntry(entry)
	tx.AddDeleteEntry(entry)

	assert.Contains(t, tx.ReadSet, "k1")
	assert.Contains(t, tx.WriteSet, "k1")
	assert.Contains(t, tx.DeleteSet, "k1")
}

func TestIsEmpty(t *testing.T) {
	tx := NewTransaction()
	assert.True(t, tx.IsEmpty())

	tx.AddWriteEntry(NewDbEntry("k", "v", false))
	assert.False(t, tx.IsEmpty())

	tx2 := NewTransaction()
	tx2.AddDeleteEntry(NewDbEntry("k", "v", false))
	assert.False(t, tx2.IsEmpty())
}

func TestConflictsWith(t *testing.T) {
	tx1 := NewTransaction()
	tx2 := NewTransaction()
	entry := NewDbEntry("k", "v", false)

	// Sin sets, no hay conflicto
	assert.False(t, tx1.ConflictsWith(tx2))

	// Conflicto por DeleteSet/WriteSet
	tx3 := NewTransaction()
	tx4 := NewTransaction()
	tx3.AddDeleteEntry(entry)
	tx4.AddWriteEntry(entry)
	assert.True(t, tx3.ConflictsWith(tx4))

	// Sin conflicto si claves no coinciden
	tx5 := NewTransaction()
	tx6 := NewTransaction()
	tx5.AddWriteEntry(NewDbEntry("a", "v", false))
	tx6.AddReadEntry(NewDbEntry("b", "v", false))
	assert.False(t, tx5.ConflictsWith(tx6))
}

func TestTransaction_Timestamp(t *testing.T) {
	tx := NewTransaction()
	now := time.Now().UnixNano()
	assert.LessOrEqual(t, tx.Timestamp, now)
}
