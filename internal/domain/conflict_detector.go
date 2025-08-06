package domain

type ConflictDetector interface {
	Check(CurrentTransactions map[string]Transaction, transaction Transaction) *Conflict
}

type ConflictFinder struct {
}

func (cf *ConflictFinder) Check(CurrentTransactions map[string]Transaction, transaction Transaction) *Conflict {
	conflict := NewConflict()
	for _, currentTransaction := range CurrentTransactions {
		if currentTransaction.Id == transaction.Id {
			continue
		}
		if currentTransaction.ConflictsWith(transaction) {
			conflict.AddTransaction(currentTransaction)
		}
	}
	if len(conflict.Transactions()) > 0 {
		conflict.AddTransaction(transaction)
		return conflict
	}
	return nil
}
