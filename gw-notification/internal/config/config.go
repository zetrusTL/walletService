package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers      string
	KafkaTopic        string
	KafkaGroupID      string
	UseConsumerGroup  bool // если false — читаем по партиции 0 без group (обход EOF с apache/kafka)
	MongoURI          string
	MongoDB           string
	MongoCollection   string
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers:     os.Getenv("KAFKA_BROKERS"),
		KafkaTopic:       os.Getenv("KAFKA_TOPIC"),
		KafkaGroupID:     os.Getenv("KAFKA_GROUP_ID"),
		UseConsumerGroup: os.Getenv("NOTIFICATION_USE_CONSUMER_GROUP") == "1" || os.Getenv("NOTIFICATION_USE_CONSUMER_GROUP") == "true",
		MongoURI:         os.Getenv("MONGO_URI"),
		MongoDB:          os.Getenv("MONGO_DB"),
		MongoCollection:  os.Getenv("MONGO_COLLECTION"),
	}
	if cfg.KafkaBrokers == "" {
		cfg.KafkaBrokers = "kafka:9092"
	}
	if cfg.KafkaTopic == "" {
		cfg.KafkaTopic = "large_transactions"
	}
	if cfg.KafkaGroupID == "" {
		cfg.KafkaGroupID = "notification-service"
	}
	if cfg.MongoDB == "" {
		cfg.MongoDB = "notification"
	}
	if cfg.MongoCollection == "" {
		cfg.MongoCollection = "large_transactions"
	}
	if cfg.MongoURI == "" {
		return nil, fmt.Errorf("missing required env MONGO_URI")
	}
	return cfg, nil
}

func (c *Config) KafkaBrokerList() []string {
	brokers := strings.Split(c.KafkaBrokers, ",")
	result := make([]string, 0, len(brokers))
	for _, b := range brokers {
		if trimmed := strings.TrimSpace(b); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
