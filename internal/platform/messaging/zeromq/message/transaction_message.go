package message

import "KVDB/internal/domain"

type TransactionMessage struct {
	Id         string                    `json:"id"`
	ReadSet    map[string]DbEntryMessage `json:"read_set"`
	WriteSet   map[string]DbEntryMessage `json:"write_set"`
	DeleteSet  map[string]DbEntryMessage `json:"delete_set"`
	Timestamp  int64                     `json:"timestamp"`
	InstanceId uint64                    `json:"instance_id"`
	Topic      string
}

type DbEntryMessage struct {
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
	Tombstone bool   `json:"tombstone,omitempty"`
}

func FromDbEntry(e domain.DbEntry) DbEntryMessage {
	return DbEntryMessage{
		e.Key(), e.Value(), e.Tombstone(),
	}
}

func (m DbEntryMessage) ToDbEntry() domain.DbEntry {
	return domain.NewDbEntry(m.Key, m.Value, m.Tombstone)
}

func TransactionMessageFrom(transaction domain.Transaction) TransactionMessage {
	return TransactionMessage{
		Id:         transaction.Id,
		ReadSet:    mapFromDbEntrySet(transaction.ReadSet),
		WriteSet:   mapFromDbEntrySet(transaction.WriteSet),
		DeleteSet:  mapFromDbEntrySet(transaction.DeleteSet),
		Timestamp:  transaction.Timestamp,
		InstanceId: transaction.InstanceId,
	}
}

func mapFromDbEntrySet(set map[string]domain.DbEntry) map[string]DbEntryMessage {
	result := make(map[string]DbEntryMessage)
	for k, e := range set {
		result[k] = FromDbEntry(e)
	}
	return result
}

func mapToDbEntrySet(set map[string]DbEntryMessage) map[string]domain.DbEntry {
	result := make(map[string]domain.DbEntry)
	for k, e := range set {
		result[k] = e.ToDbEntry()
	}
	return result
}

func (t *TransactionMessage) ToTransaction() domain.Transaction {
	return domain.Transaction{
		Id:         t.Id,
		ReadSet:    mapToDbEntrySet(t.ReadSet),
		WriteSet:   mapToDbEntrySet(t.WriteSet),
		DeleteSet:  mapToDbEntrySet(t.DeleteSet),
		Timestamp:  t.Timestamp,
		InstanceId: t.InstanceId,
	}
}
