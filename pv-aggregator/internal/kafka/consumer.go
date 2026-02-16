package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"pv-aggregator/internal/agg"
	"pv-aggregator/internal/config"
	"pv-aggregator/internal/clickhouse"
	"pv-aggregator/internal/dlq"
	"pv-aggregator/internal/model"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader      *kafka.Reader
	buffer      *agg.Buffer
	repo        *clickhouse.Repo
	dlqProducer *dlq.Producer
	cfg         *config.Config
	metrics     *Metrics
}

type Metrics struct {
	Consumed    int64
	InsertedRaw int64
	DLQCount    int64
	CHErrors    int64
	LastFlush   time.Time
	mu          sync.RWMutex
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) RecordConsumed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Consumed++
}

func (m *Metrics) RecordInserted(count int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InsertedRaw += count
}

func (m *Metrics) RecordDLQ() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DLQCount++
}

func (m *Metrics) RecordCHError() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CHErrors++
}

func (m *Metrics) RecordFlush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastFlush = time.Now()
}

func (m *Metrics) GetStats() (consumed, inserted, dlq, errors int64, lastFlush time.Time) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Consumed, m.InsertedRaw, m.DLQCount, m.CHErrors, m.LastFlush
}

func NewConsumer(cfg *config.Config, buffer *agg.Buffer, repo *clickhouse.Repo, dlqProducer *dlq.Producer) (*Consumer, error) {
	brokers := cfg.KafkaBrokerList()
	if len(brokers) == 0 {
		return nil, errors.New("no kafka brokers configured")
	}

	log.Printf("creating kafka consumer: brokers=%v, topic=%s, groupID=%s", brokers, cfg.KafkaTopic, cfg.KafkaGroupID)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           brokers,
		Topic:             cfg.KafkaTopic,
		GroupID:           cfg.KafkaGroupID,
		MinBytes:          1,
		MaxBytes:          10e6,
		MaxWait:           3 * time.Second,
		ReadBackoffMin:    100 * time.Millisecond,
		ReadBackoffMax:    1 * time.Second,
		SessionTimeout:    45 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		Logger:      kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Printf("[kafka] "+msg, args...) }),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Printf("[kafka ERROR] "+msg, args...) }),
	})

	log.Printf("kafka reader created successfully")

	return &Consumer{
		reader:      reader,
		buffer:      buffer,
		repo:        repo,
		dlqProducer: dlqProducer,
		cfg:         cfg,
		metrics:     NewMetrics(),
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	log.Printf("starting kafka consumer loop: topic=%s, groupID=%s, brokers=%v", 
		c.cfg.KafkaTopic, c.cfg.KafkaGroupID, c.cfg.KafkaBrokerList())
	
	// Start flush goroutine
	go c.flushLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "EOF") {
				log.Printf("kafka fetch EOF, reconnecting in 2s... (brokers=%v, topic=%s, groupID=%s)", c.cfg.KafkaBrokerList(), c.cfg.KafkaTopic, c.cfg.KafkaGroupID)
				time.Sleep(2 * time.Second)
				continue
			}
			log.Printf("kafka fetch error: %v (brokers=%v, topic=%s, groupID=%s)", err, c.cfg.KafkaBrokerList(), c.cfg.KafkaTopic, c.cfg.KafkaGroupID)
			time.Sleep(1 * time.Second)
			continue
		}

		log.Printf("kafka message received: partition=%d offset=%d key=%s value_len=%d", 
			msg.Partition, msg.Offset, string(msg.Key), len(msg.Value))
		c.metrics.RecordConsumed()

		// Parse and validate
		ev, validationErr := c.parseAndValidate(msg.Value)
		if validationErr != nil {
			// Send to DLQ
			if err := c.handleError(ctx, msg, validationErr.Error()); err != nil {
				log.Printf("DLQ send failed: %v", err)
				// Don't commit if DLQ failed
				continue
			}
			// Commit after DLQ success
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("commit after DLQ failed: %v", err)
			}
			continue
		}

		// Convert to row
		row := clickhouse.ConvertEventToRow(ev, msg.Offset, int32(msg.Partition))
		bufferedMsg := agg.BufferedMessage{
			Message: &msg,
			Row:     row,
		}

		// Add to buffer
		shouldFlush := c.buffer.Add(bufferedMsg)
		if shouldFlush {
			c.flush(ctx)
		}
	}
}

func (c *Consumer) parseAndValidate(data []byte) (*model.PageViewEvent, error) {
	ev, err := model.ParsePageViewEvent(data)
	if err != nil {
		return nil, fmt.Errorf("json parse: %w", err)
	}

	// Validation
	if ev.PageID == "" {
		return nil, errors.New("empty page_id")
	}
	if ev.UserID == "" {
		return nil, errors.New("empty user_id")
	}
	if ev.ViewDuration <= 0 {
		return nil, errors.New("duration <= 0")
	}
	if ev.Timestamp.IsZero() {
		return nil, errors.New("zero timestamp")
	}

	return ev, nil
}

func (c *Consumer) handleError(ctx context.Context, msg kafka.Message, errorReason string) error {
	// Send to DLQ
	if err := c.dlqProducer.Send(ctx, msg.Value, "", msg.Offset, int32(msg.Partition)); err != nil {
		return err
	}

	// Write to processing_errors table
	if err := c.repo.InsertError(ctx, string(msg.Value), errorReason, msg.Offset, int32(msg.Partition)); err != nil {
		log.Printf("insert error record failed: %v", err)
		// Don't fail DLQ if error insert fails
	}

	c.metrics.RecordDLQ()
	return nil
}

func (c *Consumer) flushLoop(ctx context.Context) {
	if c.buffer.FlushTicker() == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.buffer.FlushTicker().C:
			c.flush(ctx)
		case <-c.buffer.FlushChan():
			c.flush(ctx)
		}
	}
}

func (c *Consumer) flush(ctx context.Context) {
	messages := c.buffer.GetAndClear()
	if len(messages) == 0 {
		return
	}

	// Convert to rows
	rows := make([]clickhouse.PageViewRow, 0, len(messages))
	commits := make([]kafka.Message, 0, len(messages))

	for _, msg := range messages {
		rows = append(rows, msg.Row)
		commits = append(commits, *msg.Message)
	}

	// Insert with retry
	if err := c.insertWithRetry(ctx, rows); err != nil {
		log.Printf("flush failed after retries: %v", err)
		c.metrics.RecordCHError()
		// Don't commit on failure (at-least-once guarantee)
		return
	}

	// Commit offsets
	if err := c.reader.CommitMessages(ctx, commits...); err != nil {
		log.Printf("commit after flush failed: %v", err)
		return
	}

	c.metrics.RecordInserted(int64(len(rows)))
	c.metrics.RecordFlush()
	log.Printf("flushed %d messages to ClickHouse (offsets: %d-%d, partitions: %v)", 
		len(rows), commits[0].Offset, commits[len(commits)-1].Offset, getPartitions(commits))
}

func (c *Consumer) insertWithRetry(ctx context.Context, rows []clickhouse.PageViewRow) error {
	const maxAttempts = 5
	const baseDelay = time.Second

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := baseDelay * time.Duration(1<<uint(attempt-1)) // exponential backoff
			log.Printf("retrying ClickHouse insert (attempt %d/%d) after %v", attempt+1, maxAttempts, delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		insertCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := c.repo.InsertRawBatch(insertCtx, rows)
		cancel()

		if err == nil {
			return nil
		}

		log.Printf("ClickHouse insert error (attempt %d/%d): %v", attempt+1, maxAttempts, err)
	}

	return errors.New("max retry attempts exceeded")
}

func (c *Consumer) GetMetrics() *Metrics {
	return c.metrics
}

func getPartitions(msgs []kafka.Message) []int {
	partitions := make(map[int]bool)
	for _, msg := range msgs {
		partitions[msg.Partition] = true
	}
	result := make([]int, 0, len(partitions))
	for p := range partitions {
		result = append(result, p)
	}
	return result
}

func (c *Consumer) Close() error {
	// Final flush
	c.flush(context.Background())
	if c.buffer != nil {
		c.buffer.Stop()
	}
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}
