package strategy

import (
	"KVDB/internal/domain"
	"github.com/stretchr/testify/assert"
	"testing"
)

type mockBroadcaster struct {
	broadcastedTxs           []domain.Transaction
	broadcastedAborts        []domain.Transaction
	broadcastedCommitInits   []domain.Transaction
	broadcastedConfirmations []domain.Transaction
	broadcastedAcks          []domain.TransactionCommitAck
}

func (m *mockBroadcaster) BroadcastAck(transaction domain.TransactionCommitAck) error {
	//TODO implement me
	panic("implement me")
}

func (m *mockBroadcaster) BroadcastTransaction(tx domain.Transaction) error {
	m.broadcastedTxs = append(m.broadcastedTxs, tx)
	return nil
}
func (m *mockBroadcaster) BroadcastAbort(tx domain.Transaction) error {
	m.broadcastedAborts = append(m.broadcastedAborts, tx)
	return nil
}
func (m *mockBroadcaster) BroadcastCommitInit(tx domain.Transaction) error {
	m.broadcastedCommitInits = append(m.broadcastedCommitInits, tx)
	return nil
}
func (m *mockBroadcaster) BroadcastCommitConfirmation(tx domain.Transaction) error {
	m.broadcastedConfirmations = append(m.broadcastedConfirmations, tx)
	return nil
}

type mockCommitAckSender struct {
	sentAcks []domain.TransactionCommitAck
}

func (m *mockCommitAckSender) SendCommitAck(ack domain.TransactionCommitAck) error {
	m.sentAcks = append(m.sentAcks, ack)
	return nil
}

type mockCommitAckManager struct {
	ackedByAllInstances   bool
	hasOnlyPositiveAcks   bool
	addedAcks             []domain.TransactionCommitAck
	removedTransactionIds []string
}

func (m *mockCommitAckManager) AckedByAllInstances(transactionId string) bool {
	return m.ackedByAllInstances
}
func (m *mockCommitAckManager) HasOnlyPositiveAcks(transactionId string) bool {
	return m.hasOnlyPositiveAcks
}
func (m *mockCommitAckManager) Add(commitAck domain.TransactionCommitAck) {
	m.addedAcks = append(m.addedAcks, commitAck)
}
func (m *mockCommitAckManager) Remove(transactionId string) {
	m.removedTransactionIds = append(m.removedTransactionIds, transactionId)
}

type mockRepo struct {
	saved   []domain.DbEntry
	deleted []string
}

func (m *mockRepo) Get(key string) (domain.DbEntry, bool) {
	for _, entry := range m.saved {
		if entry.Key() == key {
			return entry, true
		}
	}
	return domain.DbEntry{}, false
}

func (m *mockRepo) Save(entry domain.DbEntry) domain.DbEntry {
	m.saved = append(m.saved, entry)
	return entry
}
func (m *mockRepo) Delete(key string) (*domain.DbEntry, bool) {
	m.deleted = append(m.deleted, key)
	return nil, true
}

func createTransactionManager(b *mockBroadcaster, cam *mockCommitAckManager, repo *mockRepo, instance *domain.DbInstance, ackSender *mockCommitAckSender) *RbTransactionManager {
	tm := &RbTransactionManager{
		CurrentTransactions:    make(map[string]domain.Transaction),
		transactionBroadcaster: b,
		conflictDetector:       &domain.ConflictFinder{},
		conflictResolver:       &domain.LWWConflictResolver{},
		commitAckManager:       cam,
		dbEntryRepository:      repo,
		currentInstance:        instance,
	}
	return tm
}

func TestTransactionManager_AddAndAbortTransaction(t *testing.T) {
	b := &mockBroadcaster{}
	cam := &mockCommitAckManager{}
	repo := &mockRepo{}
	ackSender := &mockCommitAckSender{}
	instance := &domain.DbInstance{Id: 2}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)

	tx := domain.NewTransaction()
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
	instance := &domain.DbInstance{Id: 3}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)

	tx := domain.NewTransaction()
	tx.AddWriteEntry(domain.NewDbEntry("k", "v", false))
	tx.AddDeleteEntry(domain.NewDbEntry("d", "v", false))
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
	instance := &domain.DbInstance{Id: 3}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)

	tx := domain.TransactionFromWriteEntry(domain.NewDbEntry("k", "v1", false))
	tx2 := domain.TransactionFromWriteEntry(domain.NewDbEntry("k", "v2", false))
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
	instance := &domain.DbInstance{Id: 1}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)

	//Transactions with no conflict

	tx := domain.TransactionFromWriteEntry(domain.NewDbEntry("k", "v", false))
	tm.AddTransaction(tx)
	ack := domain.NewTransactionCommitAck(tx.Id, 4, 1, true)

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
	instance := &domain.DbInstance{Id: 1}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)

	tx := domain.TransactionFromWriteEntry(domain.NewDbEntry("k", "v", false))
	tm.AddTransaction(tx)
	ack := domain.NewTransactionCommitAck(tx.Id, 5, 1, true)

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
	instance := &domain.DbInstance{Id: 1}
	tm := createTransactionManager(b, cam, repo, instance, ackSender)

	tx := domain.TransactionFromWriteEntry(domain.NewDbEntry("k", "v", false))
	tm.AddTransaction(tx)
	ack := domain.NewTransactionCommitAck(tx.Id, 5, 1, false)

	tm.AddCommitAck(ack)
	assert.Len(t, b.broadcastedAborts, 1)
	assert.Equal(t, tx.Id, b.broadcastedAborts[0].Id)
	assert.Len(t, cam.addedAcks, 0)
}
