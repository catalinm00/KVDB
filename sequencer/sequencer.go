package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/go-zeromq/zmq4"
	"log"
	"os"
)

const (
	TransactionTopic = "transaction"
)

type Sequencer struct {
	pub          zmq4.Socket
	pull         zmq4.Socket
	transactions chan zmq4.Msg
	pubPort      int
	pullPort     int
}

func NewSequencer(pubPort, pullPort int) *Sequencer {
	pub := zmq4.NewPub(context.Background())
	pull := zmq4.NewPull(context.Background())

	return &Sequencer{
		pub:          pub,
		pull:         pull,
		transactions: make(chan zmq4.Msg, 30000),
		pubPort:      pubPort,
		pullPort:     pullPort,
	}
}

func (s *Sequencer) Listen() {
	pubAddr := fmt.Sprintf("tcp://*:%d", s.pubPort)
	err := s.pub.Listen(pubAddr)
	if err != nil {
		log.Fatalf("Failed to start pub socket on %s: %v", pubAddr, err)
	}
	log.Printf("Pub socket listening on %s\n", pubAddr)

	pullAddr := fmt.Sprintf("tcp://*:%d", s.pullPort)
	err = s.pull.Listen(pullAddr)
	if err != nil {
		log.Fatalf("Failed to start pull socket on %s: %v", pullAddr, err)
	}
	log.Printf("Pull socket listening on %s\n", pullAddr)

	// Goroutine to receive messages
	go func() {
		for {
			msg, err := s.pull.Recv()
			if err != nil {
				log.Println("Error receiving message:", err)
				if errors.Is(err, zmq4.ErrClosedConn) {
					log.Println("Socket closed, exiting listener")
					return
				}
				continue
			}
			s.transactions <- msg
		}
	}()

	for msg := range s.transactions {
		err := s.pub.Send(zmq4.NewMsgFrom(
			[][]byte{
				[]byte(TransactionTopic),
				msg.Bytes(),
			}...,
		))
		if err != nil {
			log.Println("Error sending message:", err)
			return
		}
	}
}

func main() {
	pubPort := flag.Int("pub-port", 7000, "Port for PUB socket")
	pullPort := flag.Int("pull-port", 7001, "Port for PULL socket")
	flag.Parse()

	if *pubPort <= 0 || *pullPort <= 0 {
		log.Println("Ports must be positive integers")
		os.Exit(1)
	}

	seq := NewSequencer(*pubPort, *pullPort)
	seq.Listen()
}
