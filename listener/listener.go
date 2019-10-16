package listener

import (
	"fmt"

	"github.com/mrkaspa/deadletters/storage"
	"github.com/streadway/amqp"
)

// Listener listens for messages in the DLX and retries them
type Listener struct {
	maxRetries int64
	conn       *amqp.Connection
	ch         *amqp.Channel
	msgs       <-chan amqp.Delivery
	store      storage.MessageStore
}

// Create and setup a new Listener
func Create(amqpConn, dlxName string, maxRetries int64, store storage.MessageStore) (*Listener, error) {
	conn, err := amqp.Dial(amqpConn)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	err = ch.ExchangeDeclare(
		dlxName,  // name
		"direct", // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	queueDlxName := fmt.Sprintf("%s-queue", dlxName)
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
	if err != nil {
		return nil, err
	}
	err = ch.QueueBind(
		q.Name,  // queue name
		"",      // routing key
		dlxName, // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}
	l := Listener{
		maxRetries: maxRetries,
		conn:       conn,
		ch:         ch,
		msgs:       msgs,
		store:      store,
	}
	return &l, nil
}

// Run the listener
func (l *Listener) Run() {
	fmt.Println("Starting DLX Listener")
	defer l.conn.Close()
	defer l.ch.Close()

	for msg := range l.msgs {
		fmt.Printf("message arriven in DLX %+v\n", msg)
		_, count, retryQueue, ok := ExtractXDeathData(msg.Headers)
		if !ok {
			continue
		}
		if count < l.maxRetries {
			Republish(l.ch, retryQueue, msg)
		} else {
			err := l.store.Save(msg)
			if err != nil {
				fmt.Printf("error saving %s\n", err)
			}
		}
	}
}

func Republish(ch *amqp.Channel, retryQueue string, msg amqp.Delivery) error {
	// retry again
	return ch.Publish("", retryQueue, false, false, amqp.Publishing{
		Headers:         msg.Headers,
		MessageId:       msg.MessageId,
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,
		Body:            msg.Body,
	})
}

func ExtractXDeathData(headers amqp.Table) (amqp.Table, int64, string, bool) {
	xDeath, ok := extractXDeath(headers)
	if !ok {
		fmt.Println("error casting xdeath")
		return nil, 0, "", false
	}
	count, ok := extractCount(xDeath)
	if !ok {
		fmt.Println("error casting count")
		return nil, 0, "", false
	}
	retryQueue, ok := extractRoutingKey(xDeath)
	if !ok {
		fmt.Println("error casting retryq")
		return nil, 0, "", false
	}
	return xDeath, count, retryQueue, true
}

func extractXDeath(table amqp.Table) (amqp.Table, bool) {
	xDeathArr, ok := table["x-death"].([]interface{})
	if !ok {
		return nil, false
	}
	if len(xDeathArr) == 0 {
		return nil, false
	}
	xDeathNoCast := xDeathArr[0]
	xDeath, ok := xDeathNoCast.(amqp.Table)
	if !ok {
		return nil, false
	}
	return xDeath, true
}

func extractCount(data amqp.Table) (int64, bool) {
	val, ok := data["count"]
	if !ok {
		return 0, false
	}
	count, ok := val.(int64)
	if !ok {
		return 0, false
	}
	return count, true
}

func extractRoutingKey(data amqp.Table) (string, bool) {
	val, ok := data["routing-keys"]
	if !ok {
		return "", false
	}
	rkeys := val.([]interface{})
	if len(rkeys) == 0 {
		return "", false
	}
	return rkeys[0].(string), true
}
