package message

import "KVDB/internal/domain"

type AckMessage struct {
	TransactionId      string `json:"transaction_id,omitempty"`
	SenderInstanceId   uint64 `json:"sender_instance_id,omitempty"`
	ReceiverInstanceId uint64 `json:"receiver_instance_id,omitempty"`
	Timestamp          int64  `json:"timestamp,omitempty"`
	Valid              bool   `json:"valid,omitempty"`
}

func AckMessageFromCommitAck(ack domain.TransactionCommitAck) AckMessage {
	return AckMessage{
		TransactionId:      ack.TransactionId,
		SenderInstanceId:   ack.SenderInstanceId,
		ReceiverInstanceId: ack.ReceiverInstanceId,
		Timestamp:          ack.Timestamp,
		Valid:              ack.Valid,
	}
}

func (a *AckMessage) ToCommitAck() domain.TransactionCommitAck {
	return domain.TransactionCommitAck{
		TransactionId:      a.TransactionId,
		SenderInstanceId:   a.SenderInstanceId,
		ReceiverInstanceId: a.ReceiverInstanceId,
		Timestamp:          a.Timestamp,
		Valid:              a.Valid,
	}
}
