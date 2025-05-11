package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)
func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("failed to marshal value to JSON: %w", err)
	}

	// Publish the message
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}


func DeclareAndBind(
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    simpleQueueType int, // 0 = durable, 1 = transient
) (*amqp.Channel, amqp.Queue, error) {
    ch, err := conn.Channel()
    if err != nil {
        return nil, amqp.Queue{}, fmt.Errorf("cannot open channel: %w", err)
    }

    newQueue, err := ch.QueueDeclare(
		queueName,                             // name
		simpleQueueType == routing.QueueTypeDurable, // durable
		simpleQueueType != routing.QueueTypeDurable, // delete when unused
		simpleQueueType != routing.QueueTypeDurable, // exclusive
		false,                                 // no-wait
		nil,                                   // arguments
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

    err = ch.QueueBind(
        newQueue.Name,
        key,
        exchange,
        false, // noWait
        nil,   // args
    )
    if err != nil {
        return nil, amqp.Queue{}, fmt.Errorf("cannot bind queue: %w", err)
    }

    return ch, newQueue, nil
}



func SubscribeJSON[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    simpleQueueType int, 
    handler func(T) routing.AckType,
) error {
    ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
    if err != nil {
        return fmt.Errorf("failed to declare and bind queue: %w", err)
    }

    msgs, err := ch.Consume(
        queue.Name,
        "",    // consumer (auto-generated)
        false, // autoAck
        false, // exclusive
        false, // noLocal
        false, // noWait
        nil,   // args
    )
    if err != nil {
        return fmt.Errorf("failed to start consuming: %w", err)
    }

    go func() {
        for delivery := range msgs {
            var msg T
            if err := json.Unmarshal(delivery.Body, &msg); err != nil {
                _ = delivery.Ack(false)
                continue
            }
            switch handler(msg){
            case routing.Ack:
                _ = delivery.Ack(false)
            case routing.NackRequeue:
                _ = delivery.Nack(false, true)
            case routing.NackDiscard:
                _ = delivery.Nack(false, false)
            }
        }
    }()

    return nil
}