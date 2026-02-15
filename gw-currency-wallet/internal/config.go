package internal

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	AppPort                   string
	JWTSecret                 string
	DBHost                    string
	DBPort                    string
	DBUser                    string
	DBPassword                string
	DBName                    string
	DBSSLMode                 string
	ExchangerAddr             string
	ExchangerTimeout          time.Duration
	ExchangeCacheTTL          time.Duration
	KafkaBrokers              string
	KafkaTopicLargeTx         string
	LargeTransactionThreshold float64
	KafkaProducerRetries      int
	KafkaProducerBackoffMs    int
	KafkaProducerTimeoutMs    int
}

func LoadConfig() (Config, error) {
	cfg := Config{
		AppPort:                   os.Getenv("APP_PORT"),
		JWTSecret:                  os.Getenv("JWT_SECRET"),
		DBHost:                    os.Getenv("DB_HOST"),
		DBPort:                    os.Getenv("DB_PORT"),
		DBUser:                    os.Getenv("DB_USER"),
		DBPassword:                 os.Getenv("DB_PASSWORD"),
		DBName:                    os.Getenv("DB_NAME"),
		DBSSLMode:                 os.Getenv("DB_SSLMODE"),
		ExchangerAddr:             os.Getenv("EXCHANGER_ADDR"),
		ExchangerTimeout:          parseDuration(os.Getenv("EXCHANGER_TIMEOUT"), 2*time.Second),
		ExchangeCacheTTL:          parseDuration(os.Getenv("EXCHANGE_CACHE_TTL"), 10*time.Second),
		KafkaBrokers:              os.Getenv("KAFKA_BROKERS"),
		KafkaTopicLargeTx:         os.Getenv("KAFKA_TOPIC_LARGE_TRANSACTIONS"),
		LargeTransactionThreshold: parseFloat(os.Getenv("LARGE_TRANSACTION_THRESHOLD"), 40000),
		KafkaProducerRetries:      parseInt(os.Getenv("KAFKA_PRODUCER_RETRIES"), 3),
		KafkaProducerBackoffMs:    parseInt(os.Getenv("KAFKA_PRODUCER_BACKOFF_MS"), 200),
		KafkaProducerTimeoutMs:    parseInt(os.Getenv("KAFKA_PRODUCER_TIMEOUT_MS"), 2000),
	}
	if cfg.KafkaTopicLargeTx == "" {
		cfg.KafkaTopicLargeTx = "large_transactions"
	}

	if cfg.AppPort == "" {
		cfg.AppPort = "8080"
	}

	if cfg.JWTSecret == "" {
		return Config{}, fmt.Errorf("missing JWT_SECRET")
	}
	if cfg.DBHost == "" || cfg.DBPort == "" || cfg.DBUser == "" || cfg.DBName == "" {
		return Config{}, fmt.Errorf("missing db env vars")
	}
	if cfg.DBSSLMode == "" {
		cfg.DBSSLMode = "disable"
	}

	return cfg, nil
}

func parseDuration(s string, defaultVal time.Duration) time.Duration {
	if s == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return defaultVal
	}
	return d
}

func parseFloat(s string, defaultVal float64) float64 {
	if s == "" {
		return defaultVal
	}
	var f float64
	if _, err := fmt.Sscanf(s, "%f", &f); err != nil {
		return defaultVal
	}
	return f
}

func parseInt(s string, defaultVal int) int {
	if s == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return n
}

func (c Config) PostgresDSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName, c.DBSSLMode,
	)
}
