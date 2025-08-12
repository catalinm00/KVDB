package domain

type TransactionResult struct {
	TransactionId string
	ReadSet       map[string]DbEntry
	WriteSet      map[string]DbEntry
	DeleteSet     map[string]DbEntry
	Success       bool
}

func FromTransaction(t Transaction) TransactionResult {
	return TransactionResult{
		TransactionId: t.Id,
		ReadSet:       t.ReadSet,
		WriteSet:      t.WriteSet,
		DeleteSet:     t.DeleteSet,
		Success:       false,
	}
}

func (t *TransactionResult) MarkAsSuccessful() {
	t.Success = true
}
