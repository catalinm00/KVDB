package domain

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTransactionCommitAckManager_AddAndAckedByAllInstances(t *testing.T) {
	im := &DbInstanceManager{Replicas: &[]DbInstance{{Id: 1}, {Id: 2}}}
	mgr := NewTransactionCommitAckManager(im)
	ack1 := NewTransactionCommitAck("tx1", 1, 1, true)
	ack2 := NewTransactionCommitAck("tx1", 2, 1, true)

	mgr.Add(ack1)
	assert.False(t, mgr.AckedByAllInstances("tx1"))

	mgr.Add(ack2)
	assert.True(t, mgr.AckedByAllInstances("tx1"))
}

func TestTransactionCommitAckManager_HasOnlyPositiveAcks(t *testing.T) {
	im := &DbInstanceManager{}
	mgr := NewTransactionCommitAckManager(im)
	ack1 := NewTransactionCommitAck("tx2", 1, 1, true)
	ack2 := NewTransactionCommitAck("tx2", 2, 1, false)

	mgr.Add(ack1)
	assert.True(t, mgr.HasOnlyPositiveAcks("tx2"))

	mgr.Add(ack2)
	assert.False(t, mgr.HasOnlyPositiveAcks("tx2"))
}

func TestTransactionCommitAckManager_Remove(t *testing.T) {
	im := &DbInstanceManager{}
	mgr := NewTransactionCommitAckManager(im)
	ack := NewTransactionCommitAck("tx3", 1, 1, true)

	mgr.Add(ack)
	assert.True(t, mgr.HasOnlyPositiveAcks("tx3"))

	mgr.Remove("tx3")
	assert.False(t, mgr.HasOnlyPositiveAcks("tx3"))
}

func TestTransactionCommitAckHolder_AddAndCount(t *testing.T) {
	holder := NewTransactionCommitAckHolder()
	assert.Equal(t, 0, holder.CountAcks())

	holder.Add(NewTransactionCommitAck("tx4", 1, 1, true))
	assert.Equal(t, 1, holder.CountAcks())
}
