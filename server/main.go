package main

import (
	"log"

	"github.com/mrkaspa/deadletters/listener"
	"github.com/mrkaspa/deadletters/storage"
	"github.com/streadway/amqp"
)

const dlxName = "a-dlx"

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	amqpConn := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(amqpConn)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	mongoStore, err := storage.CreateMongoStore("mongodb://localhost:27017", "messages")
	failOnError(err, "Error connecting to mongo")
	listener, err := listener.Create(amqpConn, dlxName, 3, mongoStore)
	failOnError(err, "Error connecting to  rabbit")
	listener.Run()
}
