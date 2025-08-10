package publisher

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/messaging/zeromq/message"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-zeromq/zmq4"
	"log"
	"time"
)

func marshalAckMessage(ack message.AckMessage) ([]byte, error) {
	return json.Marshal(ack)
}

type ZeroMQCommitAckSender struct {
	push            map[uint64]zmq4.Socket
	instanceManager *domain.DbInstanceManager
}

func NewZeroMQCommitAckSender(instanceManager *domain.DbInstanceManager) *ZeroMQCommitAckSender {
	push := make(map[uint64]zmq4.Socket)
	return &ZeroMQCommitAckSender{
		push:            push,
		instanceManager: instanceManager,
	}
}

func (z *ZeroMQCommitAckSender) Initialize() error {
	for _, instance := range *z.instanceManager.Replicas {
		socket := zmq4.NewPush(context.Background(),
			zmq4.WithAutomaticReconnect(true),
			zmq4.WithDialerRetry(time.Second*5))
		address := fmt.Sprintf("tcp://%s:%d", instance.Host, instance.Port+8002)
		err := socket.Dial(address)
		if err != nil {
			return fmt.Errorf("failed to dial address %s: %w", address, err)
		}
		z.push[instance.Id] = socket
	}
	log.Println("ZeroMQCommitAckSender initialized with sockets for %d replicas", len(z.push))
	return nil
}

func (z *ZeroMQCommitAckSender) SendCommitAck(ack domain.TransactionCommitAck) error {
	receiverInstance := z.instanceManager.GetById(ack.ReceiverInstanceId)
	if receiverInstance == nil {
		return nil
	}
	payload, err := marshalAckMessage(message.AckMessageFromCommitAck(ack))
	msg := zmq4.NewMsgFrom(payload)
	socket, found := z.push[receiverInstance.Id]
	if !found {
		inst := z.instanceManager.GetById(ack.ReceiverInstanceId)
		if inst == nil {
			return fmt.Errorf("receiver instance %d not found", ack.ReceiverInstanceId)
		}
		newSocket := zmq4.NewPush(context.Background(),
			zmq4.WithAutomaticReconnect(true),
			zmq4.WithDialerRetry(time.Second*5))
		addr := fmt.Sprintf("tcp://%s:%d", inst.Host, inst.Port+8001)
		newSocket.Dial(addr)
		z.push[inst.Id] = newSocket
		socket = newSocket
	}
	err = socket.Send(msg)
	if err != nil {
		return err
	}
	return nil
}
