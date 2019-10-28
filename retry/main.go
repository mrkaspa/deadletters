package main

import (
	"fmt"
	"log"
	"os"

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
	mongoURL := os.Getenv("MONGODB_URL")
	mongoStore, err := storage.CreateMongoStore(mongoURL, "messages")
	failOnError(err, "Error connecting to mongo")
	defer mongoStore.Close()
	results, err := mongoStore.Retrieve(storage.MessageQuery{
		MaxRetries: 3,
	})
	failOnError(err, "Error getting results")

	amqpConn := os.Getenv("AMQP_URL")
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
