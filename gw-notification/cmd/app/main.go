package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notification/internal/config"
	"notification/internal/consumer"
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

	// Даём Kafka время полностью подняться после healthcheck.
	time.Sleep(5 * time.Second)

	// Создаём топик при старте (идемпотентно).
	ctxTopic, cancelTopic := context.WithTimeout(context.Background(), 15*time.Second)
	consumer.EnsureTopic(ctxTopic, cfg)
	cancelTopic()

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

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
	<-done

	log.Println("bye")
}
