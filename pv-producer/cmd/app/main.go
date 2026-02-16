package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"pv-producer/internal/config"
	"pv-producer/internal/generator"
	httphandler "pv-producer/internal/http"
	"pv-producer/internal/kafka"
	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Print("starting pv-producer...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Ensure topics exist
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	ensureTopics(ctx, cfg)
	cancel()

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	if producer == nil {
		log.Fatalf("kafka producer is nil")
	}
	defer producer.Close()

	gen := generator.NewGenerator(cfg.DefaultMode, cfg.DefaultRPS)

	// Start HTTP server
	httpServer := httphandler.NewServer(cfg, gen, producer)
	go func() {
		if err := httpServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Start event generation loop
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runGenerator(ctx, gen, producer, cfg)
	}()

	// Wait for interrupt
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	cancel()
	wg.Wait()
	log.Println("bye")
}

func ensureTopics(ctx context.Context, cfg *config.Config) {
	brokers := cfg.KafkaBrokerList()
	if len(brokers) == 0 {
		return
	}
	client := &kafkago.Client{
		Addr:    kafkago.TCP(brokers[0]),
		Timeout: 10 * time.Second,
	}

	topics := []string{cfg.KafkaTopic, cfg.KafkaDLQTopic}
	for _, topic := range topics {
		req := &kafkago.CreateTopicsRequest{
			Topics: []kafkago.TopicConfig{{
				Topic:             topic,
				NumPartitions:     3,
				ReplicationFactor: 1,
			}},
			ValidateOnly: false,
		}
		resp, err := client.CreateTopics(ctx, req)
		if err != nil {
			log.Printf("kafka create topic %s: %v (continuing anyway)", topic, err)
			continue
		}
		for t, err := range resp.Errors {
			if err != nil && err.Error() != "TopicExistsException" {
				log.Printf("kafka create topic %s: %v", t, err)
			} else {
				log.Printf("kafka topic %s ready", t)
			}
		}
	}
}

func runGenerator(ctx context.Context, gen *generator.Generator, producer *kafka.Producer, cfg *config.Config) {
	ticker := time.NewTicker(gen.GetInterval())
	defer ticker.Stop()

	burstActive := false
	burstCount := 0
	burstTarget := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get current mode (we'll add a method for this)
			// For now, use a simple approach
			if gen.GetMode() == "burst" {
				if !burstActive {
					// Start burst
					burstActive = true
					burstTarget = 100 + rand.Intn(900) // 100-1000 events
					burstCount = 0
					log.Printf("starting burst: %d events", burstTarget)
				}
				
				// Generate burst events rapidly
				for i := 0; i < 10 && burstCount < burstTarget; i++ {
					ev := gen.Generate()
					corruptJSON := rand.Float64() < 0.05 // 5% corrupt JSON
					if err := producer.Send(ctx, ev, corruptJSON); err != nil {
						log.Printf("send error: %v", err)
					}
					burstCount++
				}
				
				if burstCount >= burstTarget {
					burstActive = false
					burstCount = 0
					log.Printf("burst completed: %d events", burstTarget)
					// Wait before next burst
					time.Sleep(5 * time.Second)
				}
			} else {
				// Regular or night mode
				ev := gen.Generate()
				corruptJSON := rand.Float64() < 0.05 // 5% corrupt JSON
				if err := producer.Send(ctx, ev, corruptJSON); err != nil {
					log.Printf("send error: %v", err)
				}
				
				// Update ticker for mode changes
				ticker.Reset(gen.GetInterval())
			}
		}
	}
}
