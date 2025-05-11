package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	name, err := gamelogic.ClientWelcome()
	if err != nil {
        log.Fatalf("Error: %v", err)
    }
	queuename := routing.PauseKey + "." + name

	ch, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queuename, routing.PauseKey, routing.QueueTypeTransient)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer ch.Close()
	fmt.Printf("Queue %s created and bound to exchange %s with key %s\n", queue.Name, routing.ExchangePerilDirect, routing.PauseKey)
	
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

}
