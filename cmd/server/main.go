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


	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("Cannot Open Message channel")
	}
	defer publishCh.Close()

    logsQueueName := routing.GameLogSlug
    logsRoutingKey := fmt.Sprintf("%s.*", routing.GameLogSlug)

    err = pubsub.SubscribeGob(
        conn,
        string(routing.ExchangePerilTopic),
        logsQueueName,
        logsRoutingKey,
        int(routing.QueueTypeDurable),
        handleLog,
    )
    if err != nil {
		log.Fatalf("Cannot Open Message channel")
	}
    
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
                sendMessage(publishCh, true)
            case "resume":
                log.Println("Sending resume message")
                sendMessage(publishCh, false)
            case "quit":
                log.Println("Exiting...")
                break loop
            default:
                log.Println("Unknown Command")
            }
        }
    }
}


func handleLog(logEntry routing.GameLog) routing.AckType {
    defer fmt.Print("> ")
    gamelogic.WriteLog(logEntry)
    return routing.Ack
}


func sendMessage(channel *amqp.Channel, state bool) {
	err := pubsub.PublishJSON(channel, string(routing.ExchangePerilDirect), string(routing.PauseKey), routing.PlayingState{IsPaused: state})
	if err != nil {
		log.Printf("Cannot Publish messages, %v", err)
	}
}
