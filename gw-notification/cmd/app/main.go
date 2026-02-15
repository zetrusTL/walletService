package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notification/internal/config"
	"notification/internal/consumer"
	"notification/internal/health"
	"notification/internal/storage/mongo"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Print("starting notification service...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	store, err := mongo.Connect(ctx, cfg.MongoURI, cfg.MongoDB)
	cancel()
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := store.Client.Disconnect(ctx); err != nil {
			log.Printf("mongo disconnect: %v", err)
		}
		cancel()
	}()

	log.Print("mongo ping OK")

	ctxIdx, cancelIdx := context.WithTimeout(context.Background(), 15*time.Second)
	if err := store.EnsureLargeTransactionsIndex(ctxIdx, cfg.MongoCollection); err != nil {
		log.Fatalf("ensure large_transactions index: %v", err)
	}
	cancelIdx()
	log.Print("mongo index large_transactions.transaction_id OK")

	// Даём Kafka время полностью подняться после healthcheck.
	time.Sleep(5 * time.Second)

	// Создаём топик при старте (идемпотентно).
	ctxTopic, cancelTopic := context.WithTimeout(context.Background(), 15*time.Second)
	consumer.EnsureTopic(ctxTopic, cfg)
	cancelTopic()

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// HTTP server для /health
	healthHandler := health.NewHandler(store.Client, cfg)
	healthMux := http.NewServeMux()
	healthMux.Handle("/health", healthHandler)
	healthServer := &http.Server{
		Addr:         ":" + cfg.HealthPort,
		Handler:      healthMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("health server started on :%s", cfg.HealthPort)
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("health server: %v", err)
		}
	}()

	done := make(chan struct{})
	go func() {
		consumer.Run(ctx, cfg, store)
		close(done)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	cancel()

	// Graceful shutdown health server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = healthServer.Shutdown(shutdownCtx)
	shutdownCancel()

	<-done

	log.Println("bye")
}
