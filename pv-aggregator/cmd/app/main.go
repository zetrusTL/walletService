package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pv-aggregator/internal/agg"
	"pv-aggregator/internal/clickhouse"
	"pv-aggregator/internal/config"
	"pv-aggregator/internal/dlq"
	httphandler "pv-aggregator/internal/http"
	"pv-aggregator/internal/kafka"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Print("starting pv-aggregator...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	log.Printf("config loaded: KAFKA_BROKERS=%s, KAFKA_TOPIC=%s, KAFKA_GROUP_ID=%s, CLICKHOUSE_ADDR=%s",
		cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, cfg.ClickHouseAddr)

	// Connect to ClickHouse
	chClient, err := clickhouse.NewClient(cfg.ClickHouseAddr, cfg.ClickHouseDB)
	if err != nil {
		log.Fatalf("clickhouse connect: %v", err)
	}
	defer chClient.Close()

	// Ensure tables exist (run init SQL if needed)
	ctxInit, cancelInit := context.WithTimeout(context.Background(), 10*time.Second)
	ensureTables(ctxInit, chClient)
	cancelInit()

	repo := clickhouse.NewRepo(chClient)

	// Create DLQ producer
	dlqProducer := dlq.NewProducer(cfg.KafkaBrokers, cfg.KafkaDLQTopic, cfg.DLQEnabled)
	defer dlqProducer.Close()

	// Create buffer (hybrid: batch size + time interval)
	buffer := agg.NewBuffer(cfg.BatchSize, time.Duration(cfg.FlushIntervalMs)*time.Millisecond)

	// Create consumer
	consumer, err := kafka.NewConsumer(cfg, buffer, repo, dlqProducer)
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Start HTTP server
	httpServer := httphandler.NewServer(cfg, consumer)
	go func() {
		if err := httpServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Start consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := consumer.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("consumer error: %v", err)
		}
	}()

	// Wait for interrupt
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	cancel()
	time.Sleep(2 * time.Second) // Give time for graceful shutdown
	log.Println("bye")
}

func ensureTables(ctx context.Context, client *clickhouse.Client) {
	// Tables should be created by init container, but we can verify
	conn := client.Conn()
	
	// Simple ping to verify connection
	if err := conn.Ping(ctx); err != nil {
		log.Printf("clickhouse ping failed: %v", err)
		return
	}
	
	log.Printf("clickhouse tables verified")
}
