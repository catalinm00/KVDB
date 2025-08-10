package listener

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/messaging/zeromq/message"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-zeromq/zmq4"
	"log"
	"time"
)

type ZeromqCommitAckListener struct {
	pull            zmq4.Socket
	instanceManager *domain.DbInstanceManager
}

func NewZeromqCommitAckListener(instanceManager *domain.DbInstanceManager) *ZeromqCommitAckListener {
	reconnectOpt := zmq4.WithAutomaticReconnect(true)
	retryOpt := zmq4.WithDialerRetry(time.Second * 5)
	pull := zmq4.NewPull(context.Background(), reconnectOpt, retryOpt)
	listener := &ZeromqCommitAckListener{
		pull:            pull,
		instanceManager: instanceManager,
	}
	return listener
}

func (l *ZeromqCommitAckListener) Listen() {
	address := fmt.Sprintf("tcp://*:%d", l.instanceManager.CurrentInstance.Port+8002)
	err := l.pull.Listen(address)
	if err != nil {
		return
	}
	log.Println("ZeroMQ Commit Ack Listener Started. Listening on", address)
	msgCh := make(chan message.AckMessage, 2000)

	go func() {
		for {
			msg, err := l.pull.Recv()
			log.Println("ZeromqCommitAckListener received message:", msg)
			if err != nil {
				log.Fatalln("Error receiving message:", err)
				if errors.Is(err, zmq4.ErrClosedConn) {
					log.Println("Socket closed, exiting listener")
					return
				}
				continue
			}
			m, _ := unmarshalAckMessage(msg.Bytes())
			msgCh <- m
		}
	}()

	for msg := range msgCh {
		log.Println("Mensaje recibido por canal:", msg)
	}
}

func unmarshalAckMessage(data []byte) (message.AckMessage, error) {
	var ackMsg message.AckMessage
	err := json.Unmarshal(data, &ackMsg)
	if err != nil {
		return message.AckMessage{}, fmt.Errorf("error unmarshalling ack message: %w", err)
	}
	return ackMsg, nil
}
