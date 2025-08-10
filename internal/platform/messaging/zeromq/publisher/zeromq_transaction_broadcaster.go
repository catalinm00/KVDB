package publisher

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/messaging/zeromq/message"
	"context"
	"encoding/json"
	"github.com/go-zeromq/zmq4"
	"time"
)

type ZeroMQTransactionBroadcaster struct {
	pub             zmq4.Socket
	instanceManager *domain.DbInstanceManager
}

func NewZeroMQTransactionBroadcaster(im *domain.DbInstanceManager) *ZeroMQTransactionBroadcaster {
	reconnectOpt := zmq4.WithAutomaticReconnect(true)
	retryOpt := zmq4.WithDialerRetry(time.Second * 5)
	socket := zmq4.NewPub(context.Background(), reconnectOpt, retryOpt)

	return &ZeroMQTransactionBroadcaster{
		pub:             socket,
		instanceManager: im,
	}
}

const (
	TRANSACTION_TOPIC         = "transaction"
	COMMIT_INIT_TOPIC         = "commit_init"
	COMMIT_CONFIRMATION_TOPIC = "confirm_commit"
	ABORT_TOPIC               = "abort"
)

func (b *ZeroMQTransactionBroadcaster) BroadcastTransaction(transaction domain.Transaction) error {
	payload, err := MarshalTransactionMessage(message.TransactionMessageFrom(transaction))
	if err != nil {
		return err
	}
	msg := zmqMessage(TRANSACTION_TOPIC, payload)
	err = b.pub.Send(msg)
	if err != nil {
		return err
	}
	return nil
}

func (b *ZeroMQTransactionBroadcaster) BroadcastAbort(transaction domain.Transaction) error {
	payload, err := MarshalTransactionMessage(message.TransactionMessageFrom(transaction))
	if err != nil {
		return err
	}
	msg := zmqMessage(ABORT_TOPIC, payload)
	err = b.pub.Send(msg)
	if err != nil {
		return err
	}
	return nil
}

func (b *ZeroMQTransactionBroadcaster) BroadcastCommitInit(transaction domain.Transaction) error {
	payload, err := MarshalTransactionMessage(message.TransactionMessageFrom(transaction))
	if err != nil {
		return err
	}
	msg := zmqMessage(COMMIT_INIT_TOPIC, payload)
	err = b.pub.Send(msg)
	if err != nil {
		return err
	}
	return nil
}

func (b *ZeroMQTransactionBroadcaster) BroadcastCommitConfirmation(transaction domain.Transaction) error {
	payload, err := MarshalTransactionMessage(message.TransactionMessageFrom(transaction))
	if err != nil {
		return err
	}
	msg := zmqMessage(COMMIT_CONFIRMATION_TOPIC, payload)
	err = b.pub.Send(msg)
	if err != nil {
		return err
	}
	return nil
}

func zmqMessage(topic string, payload []byte) zmq4.Msg {
	msg := zmq4.NewMsgFrom(
		[][]byte{
			[]byte(topic),
			payload,
		}...,
	)
	return msg
}

func MarshalTransactionMessage(msg message.TransactionMessage) ([]byte, error) {
	return json.Marshal(msg)
}
