package storage

import (
	"context"
	"log"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoStore struct {
	client   *mongo.Client
	database string
}

// CreateMongoStore creates a store
func CreateMongoStore(mongoURL, database string) (MessageStore, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		return nil, err
	}
	return &mongoStore{client: client, database: database}, nil
}

func (m *mongoStore) Close() error {
	return m.client.Disconnect(context.Background())
}

func (m *mongoStore) Save(delivery amqp.Delivery) error {
	col := m.client.Database(m.database).Collection("msgs")
	_, err := col.InsertOne(context.Background(), deliveryToBson(delivery))
	return err
}

func (m *mongoStore) Retrieve(mq MessageQuery) ([]amqp.Publishing, error) {
	col := m.client.Database(m.database).Collection("msgs")
	cur, err := col.Find(context.Background(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(context.Background())
	results := []amqp.Publishing{}
	for cur.Next(context.Background()) {
		var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		// do something with result....
		results = append(results, bsonToPublishing(result))
	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}
	return results, nil
}

func deliveryToBson(msg amqp.Delivery) bson.M {
	return bson.M{
		"Count":           0,
		"Headers":         msg.Headers,
		"MessageId":       msg.MessageId,
		"ContentType":     msg.ContentType,
		"ContentEncoding": msg.ContentEncoding,
		"DeliveryMode":    msg.DeliveryMode,
		"CorrelationId":   msg.CorrelationId,
		"ReplyTo":         msg.ReplyTo,
		"Expiration":      msg.Expiration,
		"Timestamp":       msg.Timestamp,
		"Type":            msg.Type,
		"UserId":          msg.UserId,
		"AppId":           msg.AppId,
		"Body":            msg.Body,
	}
}

func bsonToPublishing(bson bson.M) amqp.Publishing {
	primTime := bson["Timestamp"].(primitive.DateTime)
	primBody := bson["Body"].(primitive.Binary)
	primHeaders := bson["Headers"].(primitive.M)
	headers := amqp.Table{}
	for k, v := range primHeaders {
		headers[k] = v
	}
	return amqp.Publishing{
		Headers:         headers,
		MessageId:       bson["MessageId"].(string),
		ContentType:     bson["ContentType"].(string),
		ContentEncoding: bson["ContentEncoding"].(string),
		DeliveryMode:    uint8(bson["DeliveryMode"].(int32)),
		CorrelationId:   bson["CorrelationId"].(string),
		ReplyTo:         bson["ReplyTo"].(string),
		Expiration:      bson["Expiration"].(string),
		Timestamp:       primTime.Time(),
		Type:            bson["Type"].(string),
		UserId:          bson["UserId"].(string),
		AppId:           bson["AppId"].(string),
		Body:            primBody.Data,
	}
}
