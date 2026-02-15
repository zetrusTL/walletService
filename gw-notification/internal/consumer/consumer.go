package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	mongodriver "go.mongodb.org/mongo-driver/mongo"

	"notification/internal/config"
	"notification/internal/model"
	"notification/internal/storage/mongo"
)

const eofRetryDelay = 5 * time.Second

// EnsureTopic создаёт топик, если его нет (идемпотентно). Вызывать до Run.
func EnsureTopic(ctx context.Context, cfg *config.Config) {
	brokers := cfg.KafkaBrokerList()
	if len(brokers) == 0 {
		return
	}
	client := &kafka.Client{
		Addr:    kafka.TCP(brokers[0]),
		Timeout: 10 * time.Second,
	}
	req := &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic:             cfg.KafkaTopic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}},
		ValidateOnly: false,
	}
	resp, err := client.CreateTopics(ctx, req)
	if err != nil {
		log.Printf("kafka create topic %s: %v (continuing anyway)", cfg.KafkaTopic, err)
		return
	}
	ok := true
	for topic, err := range resp.Errors {
		if err != nil {
			log.Printf("kafka create topic %s: %v", topic, err)
			ok = false
		}
	}
	if ok {
		log.Printf("kafka topic %s ready", cfg.KafkaTopic)
	}
}

// Run читает сообщения из Kafka и сохраняет в Mongo.
// По умолчанию используется простой reader (партиция 0, offset в Mongo) — без consumer group, чтобы обойти EOF с apache/kafka.
// При NOTIFICATION_USE_CONSUMER_GROUP=1 — обычный consumer group (может давать EOF с некоторыми брокерами).
func Run(ctx context.Context, cfg *config.Config, store *mongo.Store) {
	if cfg.UseConsumerGroup {
		runGroupConsumer(ctx, cfg, store)
		return
	}
	runSimpleConsumer(ctx, cfg, store)
}

func runGroupConsumer(ctx context.Context, cfg *config.Config, store *mongo.Store) {
	for {
		if ctx.Err() != nil {
			return
		}
		reader := newReader(cfg)
		runWithReader(ctx, cfg, store, reader)
		_ = reader.Close()

		if ctx.Err() != nil {
			return
		}
		log.Printf("kafka reader closed, reconnecting in %v...", eofRetryDelay)
		select {
		case <-time.After(eofRetryDelay):
		case <-ctx.Done():
			return
		}
	}
}

func runSimpleConsumer(ctx context.Context, cfg *config.Config, store *mongo.Store) {
	const partition = 0
	log.Printf("kafka consumer: simple mode (partition %d, offset in mongo)", partition)
	for {
		if ctx.Err() != nil {
			return
		}
		reader := newSimpleReader(cfg, partition)
		startOffset, err := store.GetConsumerOffset(ctx, cfg.KafkaTopic, partition)
		if err != nil {
			log.Printf("get consumer offset: %v", err)
		} else if startOffset > 0 {
			if err := reader.SetOffset(startOffset); err != nil {
				log.Printf("set offset %d: %v", startOffset, err)
			}
		} else {
			if err := reader.SetOffset(kafka.FirstOffset); err != nil {
				log.Printf("set first offset: %v", err)
			}
		}
		runWithReaderAndSaveOffset(ctx, cfg, store, reader, partition)
		_ = reader.Close()

		if ctx.Err() != nil {
			return
		}
		log.Printf("kafka reader closed, reconnecting in %v...", eofRetryDelay)
		select {
		case <-time.After(eofRetryDelay):
		case <-ctx.Done():
			return
		}
	}
}

func newSimpleReader(cfg *config.Config, partition int) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokerList(),
		Topic:   cfg.KafkaTopic,
		Partition: partition,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  3 * time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
	})
}

func newReader(cfg *config.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.KafkaBrokerList(),
		Topic:             cfg.KafkaTopic,
		GroupID:           cfg.KafkaGroupID,
		MinBytes:          1,
		MaxBytes:          10e6, // 10MB
		MaxWait:           3 * time.Second,  // короткий poll — меньше шанс, что брокер закроет idle connection
		SessionTimeout:    45 * time.Second,
		HeartbeatInterval: 2 * time.Second,  // частые heartbeat при long poll
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
	})
}

func runWithReader(ctx context.Context, cfg *config.Config, store *mongo.Store, reader *kafka.Reader) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "EOF") {
				log.Printf("kafka fetch error: EOF (connection closed), reconnecting...")
				return
			}
			log.Printf("kafka fetch error: %v", err)
			continue
		}

		var ev model.LargeTransactionEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("json unmarshal error (topic=%s partition=%d offset=%d): %v", msg.Topic, msg.Partition, msg.Offset, err)
			continue
		}

		log.Printf("consumed topic=%s partition=%d offset=%d transaction_id=%s amount=%g currency=%s",
			msg.Topic, msg.Partition, msg.Offset, ev.TransactionID, ev.Amount, ev.Currency)

		if !saveWithRetry(ctx, store, &ev, &msg, reader, cfg) {
			log.Printf("failed to save transaction_id=%s after retries, skipping commit", ev.TransactionID)
		}
	}
}

func runWithReaderAndSaveOffset(ctx context.Context, cfg *config.Config, store *mongo.Store, reader *kafka.Reader, partition int) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "EOF") {
				log.Printf("kafka fetch error: EOF (connection closed), reconnecting...")
				return
			}
			log.Printf("kafka fetch error: %v", err)
			continue
		}

		var ev model.LargeTransactionEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("json unmarshal error (topic=%s partition=%d offset=%d): %v", msg.Topic, msg.Partition, msg.Offset, err)
			continue
		}

		log.Printf("consumed topic=%s partition=%d offset=%d transaction_id=%s amount=%g currency=%s",
			msg.Topic, msg.Partition, msg.Offset, ev.TransactionID, ev.Amount, ev.Currency)

		if saveWithRetrySimple(ctx, store, &ev, cfg) {
			nextOffset := msg.Offset + 1
			if err := store.SetConsumerOffset(ctx, cfg.KafkaTopic, partition, nextOffset); err != nil {
				log.Printf("set consumer offset: %v", err)
			}
		}
	}
}

const retryAttempts = 5
const retryDelay = time.Second

func saveWithRetry(ctx context.Context, store *mongo.Store, ev *model.LargeTransactionEvent, msg *kafka.Message, reader *kafka.Reader, cfg *config.Config) bool {
	for attempt := 0; attempt < retryAttempts; attempt++ {
		if ctx.Err() != nil {
			return false
		}
		err := store.InsertLargeTransaction(ctx, cfg.MongoCollection, ev)
		if err == nil {
			if err := reader.CommitMessages(ctx, *msg); err != nil {
				log.Printf("kafka commit error: %v", err)
			}
			log.Printf("saved to mongo transaction_id=%s", ev.TransactionID)
			return true
		}
		if mongodriver.IsDuplicateKeyError(err) {
			log.Printf("duplicate event ignored")
			log.Printf("duplicate transaction ignored: %s", ev.TransactionID)
			if err := reader.CommitMessages(ctx, *msg); err != nil {
				log.Printf("kafka commit error: %v", err)
			}
			return true
		}
		log.Printf("mongo insert error (attempt %d/%d): %v", attempt+1, retryAttempts, err)
		if attempt < retryAttempts-1 {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return false
			}
		}
	}
	return false
}

func saveWithRetrySimple(ctx context.Context, store *mongo.Store, ev *model.LargeTransactionEvent, cfg *config.Config) bool {
	for attempt := 0; attempt < retryAttempts; attempt++ {
		if ctx.Err() != nil {
			return false
		}
		err := store.InsertLargeTransaction(ctx, cfg.MongoCollection, ev)
		if err == nil {
			log.Printf("saved to mongo transaction_id=%s", ev.TransactionID)
			return true
		}
		if mongodriver.IsDuplicateKeyError(err) {
			log.Printf("duplicate event ignored")
			log.Printf("duplicate transaction ignored: %s", ev.TransactionID)
			return true
		}
		log.Printf("mongo insert error (attempt %d/%d): %v", attempt+1, retryAttempts, err)
		if attempt < retryAttempts-1 {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return false
			}
		}
	}
	return false
}
