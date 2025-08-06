package domain

type ConflictResolver interface {
	Resolve(conflict Conflict) ConflictResolution
}

type LWWConflictResolver struct {
}

func (r *LWWConflictResolver) Resolve(conflict Conflict) ConflictResolution {
	mostRecentTransaction := conflict.MostRecentTransaction()
	others := conflict.Transactions()
	delete(others, mostRecentTransaction.Id)
	return ConflictResolution{
		CommitingTransactions: map[string]Transaction{
			mostRecentTransaction.Id: *mostRecentTransaction,
		},
		AbortingTransactions: others,
	}
}
