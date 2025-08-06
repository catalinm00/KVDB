package domain

type ConflictResolution struct {
	CommitingTransactions map[string]Transaction
	AbortingTransactions  map[string]Transaction
}
