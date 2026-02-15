package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"notification/internal/model"
)

type Store struct {
	Client   *mongo.Client
	Database *mongo.Database
}

func Connect(ctx context.Context, uri string, dbName string) (*Store, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	opts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}

	return &Store{
		Client:   client,
		Database: client.Database(dbName),
	}, nil
}

const offsetsCollection = "consumer_offsets"

func (s *Store) InsertLargeTransaction(ctx context.Context, collection string, ev *model.LargeTransactionEvent) error {
	coll := s.Database.Collection(collection)
	doc := bson.M{
		"transaction_id": ev.TransactionID,
		"user_id":        ev.UserID,
		"type":           ev.Type,
		"amount":         ev.Amount,
		"currency":       ev.Currency,
		"created_at":     ev.CreatedAt,
	}
	_, err := coll.InsertOne(ctx, doc)
	return err
}

func (s *Store) GetConsumerOffset(ctx context.Context, topic string, partition int) (int64, error) {
	coll := s.Database.Collection(offsetsCollection)
	id := fmt.Sprintf("%s:%d", topic, partition)
	var doc struct {
		Offset int64 `bson:"offset"`
	}
	err := coll.FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, err
	}
	return doc.Offset, nil
}

func (s *Store) SetConsumerOffset(ctx context.Context, topic string, partition int, offset int64) error {
	coll := s.Database.Collection(offsetsCollection)
	id := fmt.Sprintf("%s:%d", topic, partition)
	_, err := coll.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"offset": offset}}, options.Update().SetUpsert(true))
	return err
}
