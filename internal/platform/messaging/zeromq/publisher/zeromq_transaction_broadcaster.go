package publisher

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/messaging/zeromq/message"
	"context"
	"fmt"
	"github.com/go-zeromq/zmq4"
	json "github.com/json-iterator/go"
	"log"
	"time"
)

type ZeroMQTransactionBroadcaster struct {
	pub             zmq4.Socket
	instanceManager *domain.DbInstanceManager
}

const (
	TransactionPubPortOffset  = 8003
	TRANSACTION_TOPIC         = "transaction"
	COMMIT_INIT_TOPIC         = "commit_init"
	COMMIT_CONFIRMATION_TOPIC = "confirm_commit"
	ABORT_TOPIC               = "abort"
	ACK_TOPIC                 = "ack"
)

func NewZeroMQTransactionBroadcaster(im *domain.DbInstanceManager) *ZeroMQTransactionBroadcaster {
	reconnectOpt := zmq4.WithAutomaticReconnect(true)
	retryOpt := zmq4.WithDialerRetry(time.Second * 5)
	socket := zmq4.NewPub(context.Background(), reconnectOpt, retryOpt)

	z := &ZeroMQTransactionBroadcaster{
		pub:             socket,
		instanceManager: im,
	}
	go z.subscribeToCurrentInstance()
	return z
}

func (z *ZeroMQTransactionBroadcaster) subscribeToCurrentInstance() {
	ch := z.instanceManager.SubscribeToGetCurrentInstance()
	for range ch {
		z.Initialize()
	}
}

func (z *ZeroMQTransactionBroadcaster) Initialize() error {
	instance := z.instanceManager.CurrentInstance
	if instance == nil {
		return fmt.Errorf("Current Instance is null")
	}
	address := fmt.Sprintf("tcp://*:%d", instance.Port+TransactionPubPortOffset)
	err := z.pub.Listen(address)
	if err != nil {
		log.Println("Error starting transaction publisher", err)
		return err
	}
	log.Println("Started transaction publisher")
	return err
}

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

func (b *ZeroMQTransactionBroadcaster) BroadcastAck(transaction domain.TransactionCommitAck) error {
	payload, err := MarshalAckMessage(message.AckMessageFromCommitAck(transaction))
	if err != nil {
		return err
	}
	msg := zmqMessage(ACK_TOPIC, payload)
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
	return json.MarshalIndent(msg, "", "  ")
}

func MarshalAckMessage(ack message.AckMessage) ([]byte, error) {
	return json.Marshal(ack)
}
