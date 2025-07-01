package main

import (
	"KVDB/bootstrap"
	"log"
)

func main() {
	log.Println("Starting app...")
	_, err := bootstrap.Run()
	if err != nil {
		log.Fatal(err)
	}
}
