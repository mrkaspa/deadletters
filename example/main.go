package main

import (
	"log"
	"os"
	"strings"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	queueName := os.Getenv("EXAMPLE_QUEUE_NAME")
	dlxName := os.Getenv("DLX_NAME")
	amqpConn := os.Getenv("AMQP_URL")
	conn, err := amqp.Dial(amqpConn)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	emit(ch, queueName, dlxName)

	body := bodyFrom(os.Args)
	err = ch.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
	waitChan := make(chan int)
	<-waitChan
}

func emit(ch *amqp.Channel, queueName, dlxName string) {
	_, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		// nil,
		amqp.Table{
			"x-dead-letter-exchange":    dlxName,
			"x-dead-letter-routing-key": "",
			"x-message-ttl":             1000,
		},
	)
	failOnError(err, "Failed to declare a queue")
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}
