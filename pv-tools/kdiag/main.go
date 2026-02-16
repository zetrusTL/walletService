package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := getEnv("KAFKA_BROKERS", "kafka:9092")
	topic := getEnv("KAFKA_TOPIC", "page_views")

	fmt.Printf("=== Kafka Diagnostic Tool ===\n")
	fmt.Printf("Brokers: %s\n", brokers)
	fmt.Printf("Topic: %s\n\n", topic)

	brokerList := strings.Split(brokers, ",")
	var trimmed []string
	for _, b := range brokerList {
		if s := strings.TrimSpace(b); s != "" {
			trimmed = append(trimmed, s)
		}
	}

	if len(trimmed) == 0 {
		fmt.Fprintf(os.Stderr, "ERROR: No valid brokers specified\n")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Check topic metadata
	fmt.Println("1. Checking topic metadata...")
	client := &kafka.Client{
		Addr:    kafka.TCP(trimmed[0]),
		Timeout: 10 * time.Second,
	}

	metadataReq := &kafka.MetadataRequest{
		Topics: []string{topic},
	}

	metadataResp, err := client.GetMetadata(ctx, metadataReq)
	if err != nil {
		fmt.Printf("ERROR: Failed to get metadata: %v\n\n", err)
	} else {
		found := false
		for _, t := range metadataResp.Topics {
			if t.Name == topic {
				found = true
				fmt.Printf("✓ Topic '%s' exists\n", topic)
				fmt.Printf("  Partitions: %d\n", len(t.Partitions))
				for _, p := range t.Partitions {
					fmt.Printf("    Partition %d: Leader=%d, Replicas=%v, ISR=%v\n",
						p.ID, p.Leader.ID, p.Replicas, p.Isr)
				}
				break
			}
		}
		if !found {
			fmt.Printf("✗ Topic '%s' not found\n", topic)
		}
	}
	fmt.Println()

	// 2. Try to read messages
	fmt.Println("2. Attempting to read 5 messages from partition 0...")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  trimmed,
		Topic:    topic,
		Partition: 0,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  3 * time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
	})
	defer reader.Close()

	// Set offset to beginning
	if err := reader.SetOffset(kafka.FirstOffset); err != nil {
		fmt.Printf("ERROR: Failed to set offset: %v\n\n", err)
	} else {
		fmt.Printf("✓ Offset set to FirstOffset\n")
	}

	readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel()

	messagesRead := 0
	maxMessages := 5

	for messagesRead < maxMessages {
		msg, err := reader.FetchMessage(readCtx)
		if err != nil {
			if readCtx.Err() == context.DeadlineExceeded {
				fmt.Printf("\n⚠ Timeout reached (read %d/%d messages)\n", messagesRead, maxMessages)
				break
			}
			fmt.Printf("\nERROR: Failed to read message: %v\n", err)
			break
		}

		messagesRead++
		fmt.Printf("✓ Message %d:\n", messagesRead)
		fmt.Printf("  Offset: %d\n", msg.Offset)
		fmt.Printf("  Partition: %d\n", msg.Partition)
		fmt.Printf("  Key: %s\n", string(msg.Key))
		fmt.Printf("  Value length: %d bytes\n", len(msg.Value))
		if len(msg.Value) < 200 {
			fmt.Printf("  Value: %s\n", string(msg.Value))
		} else {
			fmt.Printf("  Value (first 200 chars): %s...\n", string(msg.Value[:200]))
		}
		fmt.Println()
	}

	if messagesRead == 0 {
		fmt.Println("⚠ No messages were read (topic might be empty or offset issue)")
	} else {
		fmt.Printf("✓ Successfully read %d message(s)\n", messagesRead)
	}
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
