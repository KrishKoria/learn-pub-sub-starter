package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func subscribe[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    simpleQueueType int,
    handler func(T) routing.AckType,
    unmarshaller func([]byte, *T) error,
) error {
    ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
    if err != nil {
        return fmt.Errorf("failed to declare and bind queue: %w", err)
    }

    err = ch.Qos(
        10,
        0,
        false,
    )
    if err != nil {
        return fmt.Errorf("failed to set QoS: %w", err)
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
        defer ch.Close() // Close the channel when the goroutine exits
        for delivery := range msgs {
            var msg T
            if err := unmarshaller(delivery.Body, &msg); err != nil {
                // Failed to unmarshal, log and NackDiscard to avoid requeueing poison messages
                fmt.Printf("Error unmarshalling message: %v. Discarding.\n", err)
                _ = delivery.Nack(false, false) // Nack and discard
                continue
            }
            switch handler(msg) {
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
    args := amqp.Table{
        "x-dead-letter-exchange": routing.ExchangePerilDLX,
    }
    newQueue, err := ch.QueueDeclare(
		queueName,                             // name
		simpleQueueType == routing.QueueTypeDurable, // durable
		simpleQueueType != routing.QueueTypeDurable, // delete when unused
		simpleQueueType != routing.QueueTypeDurable, // exclusive
		false,                                 // no-wait
		args,                                   // arguments
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
    unmarshller := func(data []byte, msg *T) error {
        return json.Unmarshal(data, msg)
    }
    return subscribe(conn, exchange, queueName, key, simpleQueueType, handler, unmarshller)
}

func PublishGOB[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buff bytes.Buffer
    encoder := gob.NewEncoder(&buff)
    if err := encoder.Encode(val); err != nil {
        return fmt.Errorf("failed to marshal value to GOB: %w", err)
    }
	err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buff.Bytes(),
	})
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}


func SubscribeGob[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    simpleQueueType int, 
    handler func(T) routing.AckType,
) error {
    unmarsheller := func(data []byte, msg *T) error {
        buffer := bytes.NewBuffer(data)
        decoder := gob.NewDecoder(buffer)
        return decoder.Decode(msg)
    }
    return subscribe(conn, exchange, queueName, key, simpleQueueType, handler, unmarsheller)
}