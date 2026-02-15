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
	writer             *kafka.Writer
	topic              string
	threshold          float64
	retries            int
	backoffMs          int
	timeoutPerAttempt  time.Duration
}

// NewPublisher создаёт publisher. Если brokers пустой — возвращает nil (publish не делаем).
// retries — число попыток (включая первую), backoffMs — базовая задержка в мс (экспонента: backoff*2^attempt), timeoutPerAttemptMs — таймаут на одну попытку в мс.
func NewPublisher(brokers string, topic string, threshold float64, retries, backoffMs, timeoutPerAttemptMs int) *Publisher {
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
	if retries <= 0 {
		retries = 3
	}
	if backoffMs <= 0 {
		backoffMs = 200
	}
	if timeoutPerAttemptMs <= 0 {
		timeoutPerAttemptMs = 2000
	}
	return &Publisher{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: trimmed,
			Topic:   topic,
		}),
		topic:              topic,
		threshold:          threshold,
		retries:            retries,
		backoffMs:          backoffMs,
		timeoutPerAttempt:  time.Duration(timeoutPerAttemptMs) * time.Millisecond,
	}
}

// Publish отправляет событие в Kafka, если amount >= threshold. Иначе ничего не делает.
// При ошибке — retry с exponential backoff; при полном провале — только WARN, ответ клиенту не ломается.
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
	msg := kafka.Message{Value: payload}
	var lastErr error
	for attempt := 0; attempt < p.retries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(p.backoffMs) * time.Millisecond
			for i := 1; i < attempt; i++ {
				backoff *= 2
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				log.Printf("WARN: kafka publish aborted transaction_id=%s: context done", ev.TransactionID)
				return
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, p.timeoutPerAttempt)
		lastErr = p.writer.WriteMessages(attemptCtx, msg)
		cancel()
		if lastErr == nil {
			if attempt > 0 {
				log.Printf("INFO: published after retry (attempts=%d) transaction_id=%s user_id=%d type=%s amount=%g %s",
					attempt+1, ev.TransactionID, ev.UserID, ev.Type, ev.Amount, ev.Currency)
			} else {
				log.Printf("kafka published large_transaction transaction_id=%s user_id=%d type=%s amount=%g %s",
					ev.TransactionID, ev.UserID, ev.Type, ev.Amount, ev.Currency)
			}
			return
		}
	}
	log.Printf("WARN: kafka publish failed transaction_id=%s: %v", ev.TransactionID, lastErr)
}

// Close закрывает writer.
func (p *Publisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}
