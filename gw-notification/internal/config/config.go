package config

import (
	"fmt"
	"os"
)

type Config struct {
	KafkaBrokers string
	KafkaTopic   string
	MongoURI     string
	MongoDB      string
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers: os.Getenv("KAFKA_BROKERS"),
		KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
		MongoURI:     os.Getenv("MONGO_URI"),
		MongoDB:      os.Getenv("MONGO_DB"),
	}
	if cfg.KafkaBrokers == "" {
		cfg.KafkaBrokers = "localhost:9092"
	}
	if cfg.KafkaTopic == "" {
		cfg.KafkaTopic = "large_transactions"
	}
	if cfg.MongoDB == "" {
		cfg.MongoDB = "notification"
	}
	if cfg.MongoURI == "" {
		return nil, fmt.Errorf("missing required env MONGO_URI")
	}
	return cfg, nil
}
