package message

import "KVDB/internal/domain"

type TransactionMessage struct {
	Id         string                    `json:"id"`
	ReadSet    map[string]domain.DbEntry `json:"read_set"`
	WriteSet   map[string]domain.DbEntry `json:"write_set"`
	DeleteSet  map[string]domain.DbEntry `json:"delete_set"`
	Timestamp  int64                     `json:"timestamp"`
	InstanceId uint64                    `json:"instance_id"`
	Topic      string
}

func TransactionMessageFrom(transaction domain.Transaction) TransactionMessage {
	return TransactionMessage{
		Id:         transaction.Id,
		ReadSet:    transaction.ReadSet,
		WriteSet:   transaction.WriteSet,
		DeleteSet:  transaction.DeleteSet,
		Timestamp:  transaction.Timestamp,
		InstanceId: transaction.InstanceId,
	}
}

func (t *TransactionMessage) ToTransaction() domain.Transaction {
	return domain.Transaction{
		Id:         t.Id,
		ReadSet:    t.ReadSet,
		WriteSet:   t.WriteSet,
		DeleteSet:  t.DeleteSet,
		Timestamp:  t.Timestamp,
		InstanceId: t.InstanceId,
	}
}
