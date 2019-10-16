package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/mrkaspa/deadletters/storage"

	"github.com/mrkaspa/deadletters/listener"

	"github.com/streadway/amqp"
)

const (
	queueName = "q-logs"
	dlxName   = "a-dlx"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	mongoStore, err := storage.CreateMongoStore("mongodb://localhost:27017", "messages")
	failOnError(err, "Error connecting to mongo")
	results, err := mongoStore.Retrieve(storage.MessageQuery{})
	failOnError(err, "Error getting results")
	for _, msg := range results {
		fmt.Printf("%+v\n", msg)
	}
}

func main1() {
	amqpConn := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(amqpConn)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	emit(ch)
	mongoStore, err := storage.CreateMongoStore("mongodb://localhost:27017", "messages")
	failOnError(err, "Error connecting to mongo")
	listener, err := listener.Create(amqpConn, dlxName, 3, mongoStore)
	failOnError(err, "Error connecting to  rabbit")
	go listener.Run()

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
	timer := time.After(60 * time.Second)
	for {
		select {
		case <-timer:
			fmt.Println("Timeout")
			break
		}
	}
}

func emit(ch *amqp.Channel) {
	_, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		true,
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
