package listener

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/config"
	"context"
	"errors"
	"fmt"
	"github.com/go-zeromq/zmq4"
	"log"
	"time"
)

type ZeromqAtomicTransactionListener struct {
	sub    zmq4.Socket
	config config.Config
	tm     domain.BasicTransactionManager
}

func NewZeromqAtomicTransactionListener(tm domain.BasicTransactionManager, config config.Config) *ZeromqAtomicTransactionListener {
	reconnectOpt := zmq4.WithAutomaticReconnect(true)
	retryOpt := zmq4.WithDialerRetry(time.Second * 2)
	sub := zmq4.NewSub(context.Background(), reconnectOpt, retryOpt)
	sub.SetOption(zmq4.OptionSubscribe, TransactionTopic)
	return &ZeromqAtomicTransactionListener{sub, config, tm}
}

func (z *ZeromqAtomicTransactionListener) Listen() {
	err := z.sub.Dial(fmt.Sprintf("tcp://%s:%d", z.config.SequencerHost, z.config.SequencerPubPort))
	if err != nil {
		return
	}

	log.Println("ZeromqAtomicTransactionListener - Started.")
	msgCh := make(chan zmq4.Msg, 20000)

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
			z.tm.AddTransaction(m.ToTransaction())
		}
	}
}
