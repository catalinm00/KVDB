package listener

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/messaging/zeromq/message"
	"context"
	"errors"
	"fmt"
	"github.com/go-zeromq/zmq4"
	json "github.com/json-iterator/go"
	"log"
	"sync"
	"time"
)

const (
	TransactionPubPortOffset = 8003
	TransactionSubPortOffset = 8004
	TransactionTopic         = "transaction"
	CommitInitTopic          = "commit_init"
	CommitConfirmationTopic  = "confirm_commit"
	AbortTopic               = "abort"
	AckTopic                 = "ack"
)

type TransactionListener interface {
	Listen()
}

type ZeromqTransactionListener struct {
	sub             zmq4.Socket
	instanceManager *domain.DbInstanceManager
	basicTM         domain.BasicTransactionManager
	rbTM            domain.ReliableBroadcastTransactionManager
	instances       map[uint64]domain.DbInstance
	mu              sync.Mutex
	autoSubscribe   bool
}

type ZmqTransactionListenerDependencies struct {
	InstanceManager         *domain.DbInstanceManager
	BasicTransactionManager domain.BasicTransactionManager
	RbTM                    domain.ReliableBroadcastTransactionManager
	AutoSubscribe           bool
}

func NewZeromqTransactionListener(deps ZmqTransactionListenerDependencies) *ZeromqTransactionListener {
	reconnectOpt := zmq4.WithAutomaticReconnect(true)
	retryOpt := zmq4.WithDialerRetry(time.Second * 2)
	sub := zmq4.NewSub(context.Background(), reconnectOpt, retryOpt)
	sub.SetOption(zmq4.OptionSubscribe, TransactionTopic)
	sub.SetOption(zmq4.OptionSubscribe, AbortTopic)
	sub.SetOption(zmq4.OptionSubscribe, CommitInitTopic)
	sub.SetOption(zmq4.OptionSubscribe, CommitConfirmationTopic)
	sub.SetOption(zmq4.OptionSubscribe, AckTopic)

	listener := &ZeromqTransactionListener{
		sub:             sub,
		instanceManager: deps.InstanceManager,
		basicTM:         deps.BasicTransactionManager,
		rbTM:            deps.RbTM,
		instances:       make(map[uint64]domain.DbInstance),
		autoSubscribe:   deps.AutoSubscribe,
	}
	listener.subscribeToInstanceChanges()
	return listener
}

func (z *ZeromqTransactionListener) subscribeToInstanceChanges() {
	sub := z.instanceManager.Subscribe()
	go func() {
		for instances := range sub {
			log.Println("Updated instances on ZeroMQTransactionListener")

			z.mu.Lock()
			z.updateSocketSubscriptions(instances)

			for _, instance := range instances {
				z.instances[instance.Id] = instance
			}

			z.mu.Unlock()
		}
	}()
}

func (z *ZeromqTransactionListener) updateSocketSubscriptions(newInstances []domain.DbInstance) {

	for _, instance := range newInstances {
		if !z.autoSubscribe && instance.Id == z.instanceManager.CurrentInstance.Id {
			continue
		}
		if _, found := z.instances[instance.Id]; !found {
			err := z.sub.Dial(fmt.Sprintf("tcp://%s:%d", instance.Host, instance.Port+TransactionPubPortOffset))
			if err != nil {
				continue
			}
		}
	}
}

func (z *ZeromqTransactionListener) Listen() {

	log.Println("ZeroMQTransactionListener - Started.")
	msgCh := make(chan zmq4.Msg, 10000)

	go func() {
		for {
			msg, err := z.sub.Recv()

			if err != nil {
				log.Println("Error receiving message:", err)
				if errors.Is(err, zmq4.ErrClosedConn) {
					log.Println("Socket closed, exiting listener")
					return
				}
				continue
			}

			msgCh <- msg
		}
	}()

	for msg := range msgCh {
		topic := string(msg.Frames[0])
		//log.Println("ZeroMQTransactionListener received message on:", topic, "\n", msg.String())
		switch topic {
		case TransactionTopic:
			m, _ := unmarshalTransactionMessage(msg.Frames[1])
			z.basicTM.AddTransaction(m.ToTransaction())
		case AbortTopic:
			m, _ := unmarshalTransactionMessage(msg.Frames[1])
			z.basicTM.AbortTransaction(m.ToTransaction().Id)
		case CommitInitTopic:
			m, _ := unmarshalTransactionMessage(msg.Frames[1])
			z.rbTM.InitCommit(m.ToTransaction())
		case AckTopic:
			m, _ := unmarshalAckMessage(msg.Frames[1])
			z.rbTM.AddCommitAck(m.ToCommitAck())
		}
	}
}

func unmarshalTransactionMessage(data []byte) (message.TransactionMessage, error) {
	var transactionMsg message.TransactionMessage
	err := json.Unmarshal(data, &transactionMsg)
	if err != nil {
		return message.TransactionMessage{}, fmt.Errorf("error unmarshalling ack message: %w", err)
	}
	return transactionMsg, nil
}

func unmarshalAckMessage(data []byte) (message.AckMessage, error) {
	var ackMsg message.AckMessage
	err := json.Unmarshal(data, &ackMsg)
	if err != nil {
		return message.AckMessage{}, fmt.Errorf("error unmarshalling ack message: %w", err)
	}
	return ackMsg, nil
}
