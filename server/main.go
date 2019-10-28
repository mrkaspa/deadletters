package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/mrkaspa/deadletters/listener"
	"github.com/mrkaspa/deadletters/storage"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok")
}

func listenConnectionError(conn *amqp.Connection) {
	errChan := conn.NotifyClose(make(chan *amqp.Error))
	for err := range errChan {
		if err != nil && err.Recover == false {
			panic(fmt.Sprintf("Connection error %s", err.Reason))
		}
	}
}

func main() {
	maxAMQPRetries, err := strconv.ParseInt(os.Getenv("MAX_AMQP_RETRIES"), 10, 64)
	failOnError(err, "Failed to get amqp retries")
	amqpConn := os.Getenv("AMQP_URL")
	dlxName := os.Getenv("DLX_NAME")
	conn, err := amqp.Dial(amqpConn)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	mongoURL := os.Getenv("MONGODB_URL")
	mongoStore, err := storage.CreateMongoStore(mongoURL, "messages")
	failOnError(err, "Error connecting to mongo")
	listener, err := listener.Create(amqpConn, dlxName, maxAMQPRetries, mongoStore)
	failOnError(err, "Error connecting to rabbit")

	//running server
	http.HandleFunc("/health", healthCheck)
	go http.ListenAndServe(":8080", nil)
	go listenConnectionError(conn)
	listener.Run()
}
