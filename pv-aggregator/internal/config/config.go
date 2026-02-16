package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers      string
	KafkaTopic        string
	KafkaDLQTopic     string
	KafkaGroupID      string
	BatchSize         int
	FlushIntervalMs   int
	ClickHouseAddr    string
	ClickHouseDB      string
	DLQEnabled        bool
	HealthPort        string
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers:    getEnv("KAFKA_BROKERS", "kafka:9092"),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "page_views"),
		KafkaDLQTopic:   getEnv("KAFKA_DLQ_TOPIC", "page_views_dlq"),
		KafkaGroupID:    getEnv("KAFKA_GROUP_ID", "pv-aggregator"),
		BatchSize:       parseInt(getEnv("BATCH_SIZE", "1000"), 1000),
		FlushIntervalMs: parseInt(getEnv("FLUSH_INTERVAL_MS", "2000"), 2000),
		ClickHouseAddr:  getEnv("CLICKHOUSE_ADDR", "clickhouse:9000"),
		ClickHouseDB:    getEnv("CLICKHOUSE_DB", "default"),
		DLQEnabled:      getEnv("DLQ_ENABLED", "true") == "true",
		HealthPort:      getEnv("HEALTH_PORT", "8083"),
	}
	
	if cfg.ClickHouseAddr == "" {
		return nil, fmt.Errorf("CLICKHOUSE_ADDR is required")
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
