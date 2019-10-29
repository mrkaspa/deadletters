package storage

import (
	"github.com/streadway/amqp"
)

// MessageQuery represents a query consult the information stored in mongo
type MessageQuery struct {
	Number     int64
	Page       int64
	MaxRetries int64
}

// MessageStore are the actions that must be realized by any persistence mechanism
type MessageStore interface {
	Close() error
	Save(amqp.Delivery) error
	Retrieve(MessageQuery) ([]amqp.Delivery, error)
}
