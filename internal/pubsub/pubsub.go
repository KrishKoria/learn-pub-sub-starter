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