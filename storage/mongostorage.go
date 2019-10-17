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
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		return nil, err
	}
	return &mongoStore{client: client, database: database}, nil
}

func (m *mongoStore) Close() error {
	return m.client.Disconnect(context.TODO())
}

func (m *mongoStore) Save(delivery amqp.Delivery) error {
	col := m.client.Database(m.database).Collection("msgs")
	if delivery.MessageId == "" {
		_, err := col.InsertOne(context.TODO(), deliveryToBson(delivery))
		return err
	}
	oid, _ := primitive.ObjectIDFromHex(delivery.MessageId)
	_, err := col.UpdateOne(context.TODO(),
		bson.M{
			"_id": bson.M{
				"$eq": oid,
			},
		},
		bson.M{
			"$inc": bson.M{
				"Count": 1,
			},
			"$set": bson.M{
				"Retrying": false,
			},
		})
	return err
}

func (m *mongoStore) Retrieve(mq MessageQuery) ([]amqp.Delivery, error) {
	col := m.client.Database(m.database).Collection("msgs")
	filter := messageQueryToFilter(mq)
	cur, err := col.Find(context.TODO(), filter)
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(context.TODO())
	results := []amqp.Delivery{}
	for cur.Next(context.TODO()) {
		var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		// do something with result....
		results = append(results, bsonToDelivery(result))
	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}
	_, err = col.UpdateMany(context.TODO(), filter, bson.M{
		"$set": bson.M{
			"Retrying": true,
		},
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func messageQueryToFilter(mq MessageQuery) bson.M {
	return bson.M{
		"Retrying": bson.M{
			"$eq": false,
		},
		"Count": bson.M{
			"$lt": mq.MaxRetries,
		},
	}
}

func deliveryToBson(msg amqp.Delivery) bson.M {
	return bson.M{
		"Count":           1,
		"Retrying":        false,
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

func bsonToDelivery(bson bson.M) amqp.Delivery {
	id := bson["_id"].(primitive.ObjectID)
	messageID := id.Hex()
	primTime := bson["Timestamp"].(primitive.DateTime)
	primBody := bson["Body"].(primitive.Binary)
	primHeaders := bson["Headers"].(primitive.M)
	headers := amqp.Table{}
	for k, v := range primHeaders {
		headers[k] = v
	}
	newXDeath := []interface{}{}
	arr := primHeaders["x-death"].(primitive.A)
	for _, elem := range arr {
		elem := elem.(primitive.M)
		newRoutingKeys := []interface{}{}
		vArray := elem["routing-keys"].(primitive.A)
		for _, va := range vArray {
			newRoutingKeys = append(newRoutingKeys, va)
		}
		elemTime := elem["time"].(primitive.DateTime)
		mapi := amqp.Table{
			"queue":        elem["queue"].(string),
			"reason":       elem["reason"].(string),
			"routing-keys": newRoutingKeys,
			"time":         elemTime.Time(),
			"count":        elem["count"].(int64),
			"exchange":     elem["exchange"].(string),
		}
		newXDeath = append(newXDeath, mapi)
	}

	headers["x-death"] = newXDeath
	return amqp.Delivery{
		Headers:         headers,
		MessageId:       messageID,
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
