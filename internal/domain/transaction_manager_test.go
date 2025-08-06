package domain

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type mockBroadcaster struct {
	broadcastedTxs           []Transaction
	broadcastedAborts        []Transaction
	broadcastedCommitInits   []Transaction
	broadcastedConfirmations []Transaction
}

func (m *mockBroadcaster) BroadcastTransaction(tx Transaction) error {
	m.broadcastedTxs = append(m.broadcastedTxs, tx)
	return nil
}
func (m *mockBroadcaster) BroadcastAbort(tx Transaction) error {
	m.broadcastedAborts = append(m.broadcastedAborts, tx)
	return nil
}
func (m *mockBroadcaster) BroadcastCommitInit(tx Transaction) error {
	m.broadcastedCommitInits = append(m.broadcastedCommitInits, tx)
	return nil
}
func (m *mockBroadcaster) BroadcastCommitConfirmation(tx Transaction) error {
	m.broadcastedConfirmations = append(m.broadcastedConfirmations, tx)
	return nil
}

type mockCommitAckSender struct {
	sentAcks []TransactionCommitAck
}

func (m *mockCommitAckSender) SendCommitAck(ack TransactionCommitAck) error {
	m.sentAcks = append(m.sentAcks, ack)
	return nil
}

type mockCommitAckManager struct {
	ackedByAllInstances   bool
	hasOnlyPositiveAcks   bool
	addedAcks             []TransactionCommitAck
	removedTransactionIds []string
}

func (m *mockCommitAckManager) AckedByAllInstances(transactionId string) bool {
	return m.ackedByAllInstances
}
func (m *mockCommitAckManager) HasOnlyPositiveAcks(transactionId string) bool {
	return m.hasOnlyPositiveAcks
}
func (m *mockCommitAckManager) Add(commitAck TransactionCommitAck) {
	m.addedAcks = append(m.addedAcks, commitAck)
}
func (m *mockCommitAckManager) Remove(transactionId string) {
	m.removedTransactionIds = append(m.removedTransactionIds, transactionId)
}

type mockRepo struct {
	saved   []DbEntry
	deleted []string
}

func (m *mockRepo) Get(key string) (DbEntry, bool) {
	for _, entry := range m.saved {
		if entry.Key() == key {
			return entry, true
		}
	}
	return DbEntry{}, false
}

func (m *mockRepo) Save(entry DbEntry) DbEntry {
	m.saved = append(m.saved, entry)
	return entry
}
func (m *mockRepo) Delete(key string) (*DbEntry, bool) {
	m.deleted = append(m.deleted, key)
	return nil, true
}

func createTransactionManager(b *mockBroadcaster, cam *mockCommitAckManager, repo *mockRepo, instance *DbInstance, ackSender *mockCommitAckSender) *TransactionManager {
	tm := &TransactionManager{
		CurrentTransactions:    make(map[string]Transaction),
		transactionBroadcaster: b,
		conflictDetector:       &ConflictFinder{},
		conflictResolver:       &LWWConflictResolver{},
		commitAckManager:       cam,
		dbEntryRepository:      repo,
		currentInstance:        instance,
		commitAckSender:        ackSender,
	}
	return tm
}

func TestTransactionManager_StartTransaction(t *testing.T) {
	b := &mockBroadcaster{}
	cam := &mockCommitAckManager{}
	repo := &mockRepo{}
	ackSender := &mockCommitAckSender{}
	instance := &DbInstance{Id: 1}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)

	tx := NewTransaction()
	tm.StartTransaction(tx)

	assert.Len(t, b.broadcastedTxs, 1)
	assert.Len(t, b.broadcastedCommitInits, 1)
	assert.Equal(t, instance.Id, tm.CurrentTransactions[tx.Id].InstanceId)
}

func TestTransactionManager_AddAndAbortTransaction(t *testing.T) {
	b := &mockBroadcaster{}
	cam := &mockCommitAckManager{}
	repo := &mockRepo{}
	ackSender := &mockCommitAckSender{}
	instance := &DbInstance{Id: 2}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)
	tm.commitAckSender = ackSender

	tx := NewTransaction()
	tm.AddTransaction(tx)
	assert.Contains(t, tm.CurrentTransactions, tx.Id)

	tm.AbortTransaction(tx.Id)
	assert.NotContains(t, tm.CurrentTransactions, tx.Id)
}

func Test_Given_WhenConfirmCommit_thenCommitTransaction(t *testing.T) {
	b := &mockBroadcaster{}
	cam := &mockCommitAckManager{}
	repo := &mockRepo{}
	ackSender := &mockCommitAckSender{}
	instance := &DbInstance{Id: 3}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)
	tm.commitAckSender = ackSender

	tx := NewTransaction()
	tx.AddWriteEntry(NewDbEntry("k", "v", false))
	tx.AddDeleteEntry(NewDbEntry("d", "v", false))
	tm.AddTransaction(tx)

	tm.ConfirmCommit(tx)

	assert.Contains(t, repo.saved, tx.WriteSet["k"])
	assert.Contains(t, repo.deleted, "d")
	assert.NotContains(t, tm.CurrentTransactions, tx.Id)
}

func Test_GivenConflict_WhenCommitInit_thenCommitNewestAndAbortOldest(t *testing.T) {
	b := &mockBroadcaster{}
	cam := &mockCommitAckManager{}
	repo := &mockRepo{}
	ackSender := &mockCommitAckSender{}
	instance := &DbInstance{Id: 3}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)
	tm.commitAckSender = ackSender

	tx := TransactionFromWriteEntry(NewDbEntry("k", "v1", false))
	tx2 := TransactionFromWriteEntry(NewDbEntry("k", "v2", false))
	tm.AddTransaction(tx)
	tm.AddTransaction(tx2)

	tm.InitCommit(tx)

	assert.Len(t, ackSender.sentAcks, 2)
	assert.Equal(t, tx.Id, ackSender.sentAcks[0].TransactionId)
	assert.Equal(t, false, ackSender.sentAcks[0].Valid)
	assert.Equal(t, tx2.Id, ackSender.sentAcks[1].TransactionId)
	assert.Equal(t, true, ackSender.sentAcks[1].Valid)
}

func Test_GivenReceivedPositiveAck_WhenNotReceivedAckFromAllInstances_AddCommitAck(t *testing.T) {
	b := &mockBroadcaster{}
	cam := &mockCommitAckManager{ackedByAllInstances: false}
	repo := &mockRepo{}
	ackSender := &mockCommitAckSender{}
	instance := &DbInstance{Id: 1}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)
	tm.commitAckSender = ackSender

	//Transactions with no conflict

	tx := TransactionFromWriteEntry(NewDbEntry("k", "v", false))
	tm.AddTransaction(tx)
	ack := NewTransactionCommitAck(tx.Id, 4, 1, true)

	tm.AddCommitAck(ack)
	assert.Len(t, b.broadcastedConfirmations, 0)
	assert.Len(t, b.broadcastedAborts, 0)
	assert.Len(t, cam.addedAcks, 1)
}

func Test_GivenPositiveAck_WhenReceiveAckFromAllInstances_thenStartCommitConfirmation(t *testing.T) {
	b := &mockBroadcaster{}
	cam := &mockCommitAckManager{ackedByAllInstances: true, hasOnlyPositiveAcks: true}
	repo := &mockRepo{}
	ackSender := &mockCommitAckSender{}
	instance := &DbInstance{Id: 1}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)
	tm.commitAckSender = ackSender

	tx := TransactionFromWriteEntry(NewDbEntry("k", "v", false))
	tm.AddTransaction(tx)
	ack := NewTransactionCommitAck(tx.Id, 5, 1, true)

	tm.AddCommitAck(ack)
	assert.Len(t, b.broadcastedAborts, 0)
	assert.Len(t, b.broadcastedConfirmations, 1)
	assert.Equal(t, tx.Id, b.broadcastedConfirmations[0].Id)
	assert.Len(t, cam.addedAcks, 1)
	assert.Equal(t, tx.Id, cam.addedAcks[0].TransactionId)
}

func Test_GivenNegativeAck_WhenReceiveAck_thenStartAbortion(t *testing.T) {
	b := &mockBroadcaster{}
	cam := &mockCommitAckManager{ackedByAllInstances: false, hasOnlyPositiveAcks: false}
	repo := &mockRepo{}
	ackSender := &mockCommitAckSender{}
	instance := &DbInstance{Id: 1}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)
	tm.commitAckSender = ackSender

	tx := TransactionFromWriteEntry(NewDbEntry("k", "v", false))
	tm.AddTransaction(tx)
	ack := NewTransactionCommitAck(tx.Id, 5, 1, false)

	tm.AddCommitAck(ack)
	assert.Len(t, b.broadcastedAborts, 1)
	assert.Equal(t, tx.Id, b.broadcastedAborts[0].Id)
	assert.Len(t, cam.addedAcks, 0)
}
