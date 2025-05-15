package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

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
        handlerMove(ch, gameState),
    )
    if err != nil {
        log.Fatalf("Error: %v", err)
    }   

    warRoutingKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
    warQueueName := "war"
    err = pubsub.SubscribeJSON(
        conn,
        string(routing.ExchangePerilTopic),
        warQueueName,
        warRoutingKey,
        int(routing.QueueTypeDurable),
        handlerWar(ch, gameState),
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
                if len(words) < 2 {
				fmt.Println("usage: spam <n>")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("error: %s is not a valid number\n", words[1])
				continue
			}
			for i := 0; i < n; i++ {
				msg := gamelogic.GetMaliciousLog()
				err = publishGameLog(ch, name, msg)
				if err != nil {
					fmt.Printf("error publishing malicious log: %s\n", err)
				}
			}
			fmt.Printf("Published %v malicious logs\n", n)
            case "quit":
                gamelogic.PrintQuit()
                return
            default:
                fmt.Println("Unknown command:", words[0])
            }
        }
    }
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) routing.AckType {
    return func(state routing.PlayingState) routing.AckType {
        defer fmt.Print("> ")
        gs.HandlePause(state)
        return routing.Ack
    }
}

func handlerMove(publishCh *amqp.Channel, gs *gamelogic.GameState) func(gamelogic.ArmyMove) routing.AckType {
    return func(move gamelogic.ArmyMove) routing.AckType {
        defer fmt.Print("> ")
        outcome := gs.HandleMove(move)
        switch outcome {
        case gamelogic.MoveOutComeSafe:
            return routing.Ack
        case gamelogic.MoveOutcomeMakeWar:
            warmsg := gamelogic.RecognitionOfWar{
                Attacker: move.Player,
                Defender: gs.GetPlayerSnap(),
            }
            warRoutingKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername())
            err := pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, warRoutingKey, warmsg)
            if err != nil {
                fmt.Println("Failed to publish war recognition:", err)
                return routing.NackRequeue
            }
            fmt.Println("War recognition published successfully!")
            return routing.Ack
        case gamelogic.MoveOutcomeSamePlayer:
            return routing.NackDiscard
        default:
            return routing.NackDiscard
    }
}
}


func handlerWar(publishCh *amqp.Channel, gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) routing.AckType {
    return func(war gamelogic.RecognitionOfWar) routing.AckType {
        defer fmt.Print("> ")
        outcome, winner, loser:= gs.HandleWar(war)
        var logMessage string 
        var ackTypeForWarMessage routing.AckType
        switch outcome {
            case gamelogic.WarOutcomeNotInvolved:
                fmt.Println("You are not involved in this war.")
                return routing.NackRequeue
            case gamelogic.WarOutcomeNoUnits:
                fmt.Println("No units in the same location. No war will be fought.")
                return routing.NackDiscard
            case gamelogic.WarOutcomeOpponentWon:
                fmt.Println("You lost the war.")
                logMessage = fmt.Sprintf("%s won the war against %s", winner, winner)
                ackTypeForWarMessage = routing.Ack
            case gamelogic.WarOutcomeYouWon:
                fmt.Println("You won the war!")
                logMessage = fmt.Sprintf("%s won the war against %s", winner, loser)
                ackTypeForWarMessage = routing.Ack
            case gamelogic.WarOutcomeDraw:
                fmt.Println("The war ended in a draw.")
                logMessage = fmt.Sprintf("The war between %s and %s ended in a draw", winner, loser)
                ackTypeForWarMessage = routing.Ack
            default:
                fmt.Println("Unknown war outcome.")
                return routing.NackDiscard
        }
        if logMessage != "" {
            gameLog := routing.GameLog{
                CurrentTime: time.Now(),
                Message:      logMessage,
                Username: war.Attacker.Username,
            }
            logRoutingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, war.Attacker.Username)
            err := pubsub.PublishGOB(publishCh, routing.ExchangePerilTopic, logRoutingKey, gameLog)
            if err != nil {
                fmt.Println("Failed to publish game log:", err)
                return routing.NackRequeue
            }
            log.Printf("Game log published successfully: %s", logMessage)
    }
        return ackTypeForWarMessage
}
}

func publishGameLog(publishCh *amqp.Channel, username, msg string) error {
	return pubsub.PublishGOB(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			Username:    username,
			CurrentTime: time.Now(),
			Message:     msg,
		},
	)
}
