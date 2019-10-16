package main

import (
	"fmt"
	"log"

	"github.com/mrkaspa/deadletters/listener"
	"github.com/mrkaspa/deadletters/storage"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	mongoStore, err := storage.CreateMongoStore("mongodb://localhost:27017", "messages")
	failOnError(err, "Error connecting to mongo")
	defer mongoStore.Close()
	results, err := mongoStore.Retrieve(storage.MessageQuery{})
	failOnError(err, "Error getting results")

	amqpConn := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(amqpConn)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	fmt.Printf("Found %d messages to retry\n", len(results))
	for _, msg := range results {
		xDeath, _, retryQueue, ok := listener.ExtractXDeathData(msg.Headers)
		if !ok {
			continue
		}
		xDeath["count"] = 0
		msg.Headers["x-death"] = xDeath
		fmt.Println("Republishing message")
		err := listener.Republish(ch, retryQueue, msg)
		failOnError(err, "Failed to publish a message")
	}
}
