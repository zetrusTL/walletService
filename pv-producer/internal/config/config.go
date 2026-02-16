package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers   string
	KafkaTopic     string
	KafkaDLQTopic  string
	HTTPPort       string
	DefaultMode    string // regular, burst, night
	DefaultRPS     int
	BatchSize      int
	FlushIntervalMs int
	SendMode       string // sync, async, batch
	PartitionStrategy string // key, rr, random
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers:   getEnv("KAFKA_BROKERS", "kafka:9092"),
		KafkaTopic:     getEnv("KAFKA_TOPIC", "page_views"),
		KafkaDLQTopic:  getEnv("KAFKA_DLQ_TOPIC", "page_views_dlq"),
		HTTPPort:       getEnv("HTTP_PORT", "8082"),
		DefaultMode:    getEnv("DEFAULT_MODE", "regular"),
		DefaultRPS:     parseInt(getEnv("DEFAULT_RPS", "5"), 5),
		BatchSize:      parseInt(getEnv("BATCH_SIZE", "200"), 200),
		FlushIntervalMs: parseInt(getEnv("FLUSH_MS", "1000"), 1000),
		SendMode:       getEnv("SEND_MODE", "batch"),
		PartitionStrategy: getEnv("PARTITION_STRATEGY", "key"),
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

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func parseInt(s string, defaultValue int) int {
	if s == "" {
		return defaultValue
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return defaultValue
	}
	return v
}
