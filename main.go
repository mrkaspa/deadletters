package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

const (
	queueName    = "q-logs"
	dlxName      = "a-dlx"
	queueDlxName = "q-dlx"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	emit(ch)
	msgChann := deadListener(ch)

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
	timer := time.After(10 * time.Second)
	for {
		select {
		case <-timer:
			fmt.Println("Timeout")
			goto end
		case msg := <-msgChann:
			fmt.Println("Incoming message in dead letter")
			fmt.Printf("%+v\n", msg)
			time.Sleep(5 * time.Second)
			// goto end
		}
	}
end:
}

func emit(ch *amqp.Channel) {
	q, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		true,
		false,
		// nil,
		amqp.Table{
			"x-dead-letter-exchange":    dlxName,
			"x-dead-letter-routing-key": "",
		},
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to bind a queue")

	go func() {
		fmt.Println("Listening for msgs")
		for msg := range msgs {
			retry := true
			if msg.DeliveryTag > 2 {
				retry = false
			}
			fmt.Println("Message incomming in logs-queue")
			fmt.Printf("retry? %t\n", retry)
			// fmt.Printf("%+v\n", msg)

			msg.Nack(false, retry)
		}
	}()
}

func deadListener(ch *amqp.Channel) <-chan amqp.Delivery {
	err := ch.ExchangeDeclare(
		dlxName,  // name
		"direct", // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		queueDlxName,
		false,
		false,
		true,
		false,
		amqp.Table{
			"x-message-ttl": 10000,
		},
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,  // queue name
		"",      // routing key
		dlxName, // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to bind a queue")

	fmt.Println("Waiting for dead letters")
	return msgs
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
