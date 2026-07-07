package queue

import (
	"errors"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection
var RabbitConn *amqp.Connection

// Creating a channel
var RabbitChannel *amqp.Channel

func ConnetRabbitMQ() error {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		return errors.New("RABBITMQ_URL is not present")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	queueName := os.Getenv("REPLICA_REPAIR_QUEUE")
    if queueName == "" {
        return errors.New("QueueName is not present")
    }

	_, err = channel.QueueDeclare(
		queueName,
		true,      // durable: survive RabbitMQ restart
		false,     // autoDelete: do not delete when consumers disconnect
		false,     // exclusive: shared by multiple services
		false,     // noWait: wait for RabbitMQ confirmation
		nil,       // args: no extra options
	)
	
	if err != nil {
		channel.Close()
		conn.Close()
		return err
	}

	RabbitChannel = channel
	RabbitConn = conn
	return nil
}
