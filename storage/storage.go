package storage

import (
	"github.com/streadway/amqp"
)

type MessageQuery struct {
	Number     int64
	Page       int64
	MaxRetries int64
}

type MessageStore interface {
	Close() error
	Save(amqp.Delivery) error
	Retrieve(MessageQuery) ([]amqp.Delivery, error)
}
