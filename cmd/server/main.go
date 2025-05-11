package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")
	msg, err := conn.Channel()
	if err != nil {
		log.Fatalf("Cannot Open Message channel")
	}
	defer msg.Close()

    logsQueueName := routing.GameLogSlug
    logsRoutingKey := routing.GameLogSlug + ".*"
    logsCh, logsQueue, err := pubsub.DeclareAndBind(
        conn,
        routing.ExchangePerilTopic,
        logsQueueName,
        logsRoutingKey,
        routing.QueueTypeDurable,
    )
    if err != nil {
        log.Fatalf("Failed to declare and bind logs queue: %v", err)
    }
    defer logsCh.Close()
    fmt.Printf("Queue %s created and bound to exchange %s with key %s\n", logsQueue.Name, routing.ExchangePerilTopic, logsRoutingKey)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	
	gamelogic.PrintServerHelp()
	loop:
    for {
        select {
        case <-sigChan:
            fmt.Println("Received shutdown signal, shutting down...")
            break loop
        default:
            words := gamelogic.GetInput()
            if len(words) == 0 {
                continue
            }
            switch words[0] {
            case "pause":
                log.Println("Sending pause message")
                sendMessage(msg, true)
            case "resume":
                log.Println("Sending resume message")
                sendMessage(msg, false)
            case "quit":
                log.Println("Exiting...")
                break loop
            default:
                log.Println("Unknown Command")
            }
        }
    }
}


func sendMessage(channel *amqp.Channel, state bool) {
	err := pubsub.PublishJSON(channel, string(routing.ExchangePerilDirect), string(routing.PauseKey), routing.PlayingState{IsPaused: state})
	if err != nil {
		log.Fatalf("Cannot Publish messages, %v", err)
	}
}