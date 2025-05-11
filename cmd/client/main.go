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

    moveRoutingKey := routing.ArmyMovesPrefix + "." + "*"
    moveQueuename := routing.ArmyMovesPrefix + "." + name
    ch, queue, err := pubsub.DeclareAndBind(
        conn,
        routing.ExchangePerilDirect,
        queuename,
        routing.PauseKey,
        routing.QueueTypeTransient,
    )
    if err != nil {
        log.Fatalf("Error: %v", err)
    }
    defer ch.Close()
    fmt.Printf("Queue %s created and bound to exchange %s with key %s\n", queue.Name, routing.ExchangePerilDirect, routing.PauseKey)

    gameState := gamelogic.NewGameState(name)
    err = pubsub.SubscribeJSON(
        conn,
        string(routing.ExchangePerilDirect),
        queuename,
        string(routing.PauseKey),
        int(routing.QueueTypeTransient),
        handlerPause(gameState),
    )
    if err != nil {
        log.Fatalf("Error: %v", err)
    }

    err = pubsub.SubscribeJSON(
        conn,
        string(routing.ExchangePerilTopic),
        moveQueuename,
        moveRoutingKey,
        int(routing.QueueTypeTransient),
        handlerMove(gameState),
    )
    if err != nil {
        log.Fatalf("Error: %v", err)
    }

    signalChan := make(chan os.Signal, 1)
    signal.Notify(signalChan, os.Interrupt)

    for {
        select {
        case <-signalChan:
            gamelogic.PrintQuit()
            return
        default:
            words := gamelogic.GetInput()
            if len(words) == 0 {
                continue
            }
            switch words[0] {
            case "spawn":
                gameState.CommandSpawn(words)
            case "move":
                move, err := gameState.CommandMove(words)
                if err != nil {
                    log.Fatalf("Error: %v", err)
                }
                routingKey := routing.ArmyMovesPrefix + "." + name
                err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routingKey, move)
                if err != nil {
                    fmt.Println("Failed to publish move:", err)
                } else {
                    fmt.Println("Move published successfully!")
                }
            case "status":
                gameState.CommandStatus()
            case "help":
                gamelogic.PrintClientHelp()
            case "spam":
                fmt.Println("Spamming not allowed yet!")
            case "quit":
                gamelogic.PrintQuit()
                return
            default:
                fmt.Println("Unknown command:", words[0])
            }
        }
    }
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
    return func(state routing.PlayingState) {
        defer fmt.Print("> ")
        gs.HandlePause(state)
    }
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
    return func(move gamelogic.ArmyMove) {
        defer fmt.Print("> ")
        gs.HandleMove(move)
    }
}