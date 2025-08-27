package domain

type ConflictResolver interface {
	Resolve(conflict Conflict) ConflictResolution
}

type LWWConflictResolver struct {
}

func (r *LWWConflictResolver) Resolve(conflict Conflict) ConflictResolution {
	mostRecentTransaction := conflict.MostRecentTransaction()
	others := CopyMap(conflict.Transactions())
	delete(others, mostRecentTransaction.Id)
	return ConflictResolution{
		CommitingTransactions: map[string]Transaction{
			mostRecentTransaction.Id: *mostRecentTransaction,
		},
		AbortingTransactions: others,
	}
}

type FWWConflictResolver struct {
}

func (r *FWWConflictResolver) Resolve(conflict Conflict) ConflictResolution {
	transactions := CopyMap(conflict.Transactions())
	oldest := conflict.OldestTransaction()
	delete(transactions, oldest.Id)
	return ConflictResolution{
		CommitingTransactions: map[string]Transaction{
			oldest.Id: *oldest,
		},
		AbortingTransactions: transactions,
	}
}

func CopyMap[K comparable, V any](m map[K]V) map[K]V {
	newMap := make(map[K]V, len(m))
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}
