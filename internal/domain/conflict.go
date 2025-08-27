package domain

type Conflict struct {
	transactions map[string]Transaction
}

func NewConflict() *Conflict {
	return &Conflict{
		transactions: make(map[string]Transaction),
	}
}

func (c *Conflict) AddTransaction(transaction Transaction) {
	c.transactions[transaction.Id] = transaction
}

func (c *Conflict) MostRecentTransaction() *Transaction {
	var mostRecent *Transaction
	for _, transaction := range c.transactions {
		if mostRecent == nil || transaction.Timestamp > mostRecent.Timestamp {
			mostRecent = &transaction
		}
	}
	return mostRecent
}

func (c *Conflict) OldestTransaction() *Transaction {
	var oldest *Transaction
	for _, transaction := range c.transactions {
		if oldest == nil || transaction.Timestamp < oldest.Timestamp {
			oldest = &transaction
		}
	}
	return oldest
}

func (c *Conflict) Transactions() map[string]Transaction {
	return c.transactions
}
