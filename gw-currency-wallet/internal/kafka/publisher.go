package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// LargeTransactionEvent — формат сообщения в топик large_transactions (как ожидает gw-notification).
type LargeTransactionEvent struct {
	TransactionID string  `json:"transaction_id"`
	UserID        int     `json:"user_id"`
	Type          string  `json:"type"` // "deposit"|"withdraw"|"exchange"
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	CreatedAt     string  `json:"created_at"` // RFC3339
}

// Publisher публикует события крупных транзакций в Kafka.
type Publisher struct {
	writer    *kafka.Writer
	topic     string
	threshold float64
}

// NewPublisher создаёт publisher. Если brokers пустой — возвращает nil (publish не делаем).
func NewPublisher(brokers string, topic string, threshold float64) *Publisher {
	brokerList := strings.Split(brokers, ",")
	var trimmed []string
	for _, b := range brokerList {
		if s := strings.TrimSpace(b); s != "" {
			trimmed = append(trimmed, s)
		}
	}
	if len(trimmed) == 0 || topic == "" {
		return nil
	}
	return &Publisher{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: trimmed,
			Topic:   topic,
		}),
		topic:     topic,
		threshold: threshold,
	}
}

// Publish отправляет событие в Kafka, если amount >= threshold. Иначе ничего не делает.
func (p *Publisher) Publish(ctx context.Context, userID int64, opType, currency string, amount float64) {
	if p == nil || amount < p.threshold {
		return
	}
	ev := LargeTransactionEvent{
		TransactionID: uuid.New().String(),
		UserID:        int(userID),
		Type:          opType,
		Amount:        amount,
		Currency:      currency,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		log.Printf("kafka marshal large transaction: %v", err)
		return
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{Value: payload}); err != nil {
		log.Printf("kafka write large_transactions: %v", err)
		return
	}
	log.Printf("kafka published large_transaction transaction_id=%s user_id=%d type=%s amount=%g %s", ev.TransactionID, ev.UserID, ev.Type, ev.Amount, ev.Currency)
}

// Close закрывает writer.
func (p *Publisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
