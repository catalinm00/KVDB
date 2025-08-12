package publisher

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/messaging/zeromq/message"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-zeromq/zmq4"
	"log"
	"sync"
	"time"
)

func marshalAckMessage(ack message.AckMessage) ([]byte, error) {
	return json.Marshal(ack)
}

type ZeroMQCommitAckSender struct {
	push            map[uint64]zmq4.Socket
	instanceManager *domain.DbInstanceManager
	instances       map[uint64]domain.DbInstance
	mu              sync.Mutex
}

const (
	PullPortOffset = 8000
	PushPortOffset = 8001
)

func NewZeroMQCommitAckSender(instanceManager *domain.DbInstanceManager) *ZeroMQCommitAckSender {
	push := make(map[uint64]zmq4.Socket)
	sender := &ZeroMQCommitAckSender{
		push:            push,
		instanceManager: instanceManager,
		instances:       make(map[uint64]domain.DbInstance),
	}
	sender.subscribe()
	return sender
}

func (z *ZeroMQCommitAckSender) subscribe() {
	sub := z.instanceManager.Subscribe()
	go func() {
		for instances := range sub {
			z.mu.Lock()
			log.Println("Updated instances on NewZeroMQCommitAckSender")

			for _, instance := range instances {
				z.instances[instance.Id] = instance
			}
			z.Initialize()
			z.mu.Unlock()
		}
	}()
}

func (z *ZeroMQCommitAckSender) Initialize() error {
	for _, instance := range *z.instanceManager.Replicas {
		if z.push[instance.Id] != nil {
			continue
		}
		socket := zmq4.NewPush(context.Background(),
			zmq4.WithAutomaticReconnect(true),
			zmq4.WithDialerRetry(time.Second*5))
		address := fmt.Sprintf("tcp://%s:%d", instance.Host, instance.Port+PullPortOffset)
		err := socket.Dial(address)
		if err != nil {
			return fmt.Errorf("failed to dial address %s: %w", address, err)
		}
		z.push[instance.Id] = socket
	}
	for key, socket := range z.push {
		if _, found := z.instances[key]; found {
			continue
		}
		socket.Close()
		delete(z.push, key)
	}
	return nil
}

func (z *ZeroMQCommitAckSender) SendCommitAck(ack domain.TransactionCommitAck) error {
	receiverInstance := z.instanceManager.GetById(ack.ReceiverInstanceId)
	if receiverInstance == nil {
		return nil
	}
	payload, err := marshalAckMessage(message.AckMessageFromCommitAck(ack))
	msg := zmq4.NewMsgFrom(payload)
	socket, _ := z.push[receiverInstance.Id]
	err = socket.Send(msg)
	if err != nil {
		return err
	}
	return nil
}
