package dlq

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer  *kafka.Writer
	enabled bool
}

func NewProducer(brokers string, topic string, enabled bool) *Producer {
	if !enabled || brokers == "" || topic == "" {
		return &Producer{enabled: false}
	}

	brokerList := strings.Split(brokers, ",")
	var trimmed []string
	for _, b := range brokerList {
		if s := strings.TrimSpace(b); s != "" {
			trimmed = append(trimmed, s)
		}
	}

	if len(trimmed) == 0 {
		return &Producer{enabled: false}
	}

	return &Producer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: trimmed,
			Topic:   topic,
		}),
		enabled: true,
	}
}

func (p *Producer) Send(ctx context.Context, rawMessage []byte, pageID string, kafkaOffset int64, kafkaPartition int32) error {
	if !p.enabled {
		return nil
	}

	msg := kafka.Message{
		Key:   []byte("dlq"),
		Value: rawMessage,
		Headers: []kafka.Header{
			{Key: "original_offset", Value: []byte(fmt.Sprintf("%d", kafkaOffset))},
			{Key: "original_partition", Value: []byte(fmt.Sprintf("%d", kafkaPartition))},
			{Key: "page_id", Value: []byte(pageID)},
		},
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		log.Printf("DLQ send failed: %v", err)
		return err
	}

	log.Printf("sent to DLQ: offset=%d partition=%d", kafkaOffset, kafkaPartition)
	return nil
}

func (p *Producer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
