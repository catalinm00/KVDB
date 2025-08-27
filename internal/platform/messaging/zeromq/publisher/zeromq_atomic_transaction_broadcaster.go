package publisher

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/config"
	"KVDB/internal/platform/messaging/zeromq/message"
	"context"
	"fmt"
	"github.com/go-zeromq/zmq4"
	"log"
	"time"
)

type AtomicTransactionBroadcaster struct {
	push   zmq4.Socket
	config config.Config
}

func NewAtomicBroadcaster(config config.Config) *AtomicTransactionBroadcaster {
	reconnectOpt := zmq4.WithAutomaticReconnect(true)
	retryOpt := zmq4.WithDialerRetry(time.Second * 5)
	socket := zmq4.NewPush(context.Background(), reconnectOpt, retryOpt)
	return &AtomicTransactionBroadcaster{
		push:   socket,
		config: config,
	}
}

func (a *AtomicTransactionBroadcaster) Initialize() {
	err := a.push.Dial(fmt.Sprintf("tcp://%s:%d", a.config.SequencerHost, a.config.SequencerPullPort))
	if err != nil {
		log.Println("AtomicBroadcaster suffered an error", err)
		return
	}
	log.Println("AtomicBroadcaster Started")
}

func (a *AtomicTransactionBroadcaster) BroadcastTransaction(transaction domain.Transaction) error {
	msg := message.TransactionMessageFrom(transaction)
	payload, _ := MarshalTransactionMessage(msg)
	err := a.push.Send(zmq4.NewMsg(payload))
	if err != nil {
		log.Println("Error sending message")
		return err
	}
	return nil
}

func (a AtomicTransactionBroadcaster) BroadcastAbort(transaction domain.Transaction) error {
	//TODO implement me
	panic("implement me")
}

func (a AtomicTransactionBroadcaster) BroadcastCommitInit(transaction domain.Transaction) error {
	//TODO implement me
	panic("implement me")
}

func (a AtomicTransactionBroadcaster) BroadcastCommitConfirmation(transaction domain.Transaction) error {
	//TODO implement me
	panic("implement me")
}

func (a AtomicTransactionBroadcaster) BroadcastAck(transaction domain.TransactionCommitAck) error {
	//TODO implement me
	panic("implement me")
}
