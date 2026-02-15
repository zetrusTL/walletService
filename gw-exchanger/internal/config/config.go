package config

import (
	"fmt"
	"os"
)

type Config struct {
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string
	GRPCPort   string
}

func Load() (*Config, error) {
	cfg := &Config{
		DBHost:     os.Getenv("DB_HOST"),
		DBPort:     os.Getenv("DB_PORT"),
		DBUser:     os.Getenv("DB_USER"),
		DBPassword: os.Getenv("DB_PASSWORD"),
		DBName:     os.Getenv("DB_NAME"),
		DBSSLMode:  os.Getenv("DB_SSLMODE"),
		GRPCPort:   os.Getenv("GRPC_PORT"),
	}
	if cfg.DBPort == "" {
		cfg.DBPort = "5432"
	}
	if cfg.GRPCPort == "" {
		cfg.GRPCPort = "9090"
	}
	if cfg.DBSSLMode == "" {
		cfg.DBSSLMode = "disable"
	}
	if cfg.DBHost == "" || cfg.DBUser == "" || cfg.DBName == "" {
		return nil, fmt.Errorf("missing required DB env (DB_HOST, DB_USER, DB_NAME)")
	}
	return cfg, nil
}

func (c *Config) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName, c.DBSSLMode,
	)
}
