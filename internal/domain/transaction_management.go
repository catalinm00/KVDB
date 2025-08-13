package domain

type TransactionExecutionStrategy interface {
	Execute(t Transaction) <-chan TransactionResult
	AddTransaction(transaction Transaction)
	AbortTransaction(id string)
}

type BasicTransactionManager interface {
	AddTransaction(transaction Transaction)
	AbortTransaction(id string)
}

type ReliableBroadcastTransactionManager interface {
	InitCommit(t Transaction)
	ConfirmCommit(t Transaction)
	AddCommitAck(ack TransactionCommitAck)
}

type TransactionBroadcaster interface {
	BroadcastTransaction(transaction Transaction) error
	BroadcastAbort(transaction Transaction) error
	BroadcastCommitInit(transaction Transaction) error
	BroadcastCommitConfirmation(transaction Transaction) error
	BroadcastAck(transaction TransactionCommitAck) error
}

type CommitAckSender interface {
	SendCommitAck(ack TransactionCommitAck) error
}
