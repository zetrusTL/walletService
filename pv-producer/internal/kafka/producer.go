package kafka

import (
	"context"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"pv-producer/internal/config"
	"pv-producer/internal/model"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer            *kafka.Writer
	brokers           []string
	topic             string
	dlqTopic          string
	sendMode          string // sync, async, batch
	batchSize         int
	flushInterval     time.Duration
	partitionStrategy string // key, rr, random
	batch             []kafka.Message
	batchMu           sync.Mutex
	flushTicker       *time.Ticker
	stopFlush         chan struct{}
	metrics           *Metrics
}

type Metrics struct {
	SentCount     int64
	FailedCount   int64
	Latencies     []time.Duration
	mu            sync.RWMutex
}

func NewMetrics() *Metrics {
	return &Metrics{
		Latencies: make([]time.Duration, 0, 1000),
	}
}

func (m *Metrics) RecordSent(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SentCount++
	if len(m.Latencies) < 1000 {
		m.Latencies = append(m.Latencies, latency)
	} else {
		// Keep last 1000
		m.Latencies = append(m.Latencies[1:], latency)
	}
}

func (m *Metrics) RecordFailed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FailedCount++
}

func (m *Metrics) GetStats() (sent, failed int64, avgLatency, p95Latency time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sent = m.SentCount
	failed = m.FailedCount
	
	if len(m.Latencies) == 0 {
		return
	}
	
	var sum time.Duration
	for _, l := range m.Latencies {
		sum += l
	}
	avgLatency = sum / time.Duration(len(m.Latencies))
	
	// Simple P95: sort and take 95th percentile
	if len(m.Latencies) > 0 {
		sorted := make([]time.Duration, len(m.Latencies))
		copy(sorted, m.Latencies)
		// Simple approximation: take 95th percentile
		idx := int(float64(len(sorted)) * 0.95)
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		p95Latency = sorted[idx]
	}
	return
}

func NewProducer(cfg *config.Config) (*Producer, error) {
	brokers := cfg.KafkaBrokerList()
	if len(brokers) == 0 {
		return nil, nil
	}

	p := &Producer{
		brokers:           brokers,
		topic:             cfg.KafkaTopic,
		dlqTopic:          cfg.KafkaDLQTopic,
		sendMode:          cfg.SendMode,
		batchSize:         cfg.BatchSize,
		flushInterval:     time.Duration(cfg.FlushIntervalMs) * time.Millisecond,
		partitionStrategy: cfg.PartitionStrategy,
		batch:             make([]kafka.Message, 0, cfg.BatchSize),
		stopFlush:         make(chan struct{}),
		metrics:           NewMetrics(),
	}

	writerConfig := kafka.WriterConfig{
		Brokers: brokers,
		Topic:   cfg.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
		BatchSize: cfg.BatchSize,
		BatchTimeout: time.Duration(cfg.FlushIntervalMs) * time.Millisecond,
		RequiredAcks: 1, // RequireOne
		Async: cfg.SendMode == "async",
	}

	p.writer = kafka.NewWriter(writerConfig)

	// Start batch flusher if in batch mode
	if cfg.SendMode == "batch" {
		p.flushTicker = time.NewTicker(p.flushInterval)
		go p.flushLoop()
	}

	return p, nil
}

func (p *Producer) flushLoop() {
	for {
		select {
		case <-p.flushTicker.C:
			p.Flush(context.Background())
		case <-p.stopFlush:
			return
		}
	}
}

func (p *Producer) Send(ctx context.Context, ev *model.PageViewEvent, corruptJSON bool) error {
	start := time.Now()
	
	// 5% error: corrupt JSON
	var payload []byte
	var err error
	if corruptJSON {
		payload = []byte(`{"invalid": json}`)
	} else {
		payload, err = ev.ToJSON()
		if err != nil {
			p.metrics.RecordFailed()
			return err
		}
	}

	msg := kafka.Message{
		Key:   []byte(p.getPartitionKey(ev)),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "event_id", Value: []byte(ev.EventID)},
		},
	}

	switch p.sendMode {
	case "sync":
		return p.sendSync(ctx, msg, start)
	case "async":
		return p.sendAsync(ctx, msg, start)
	case "batch":
		return p.sendBatch(ctx, msg, start)
	default:
		return p.sendSync(ctx, msg, start)
	}
}

func (p *Producer) getPartitionKey(ev *model.PageViewEvent) string {
	switch p.partitionStrategy {
	case "key":
		return ev.PageID
	case "rr":
		return "" // Round-robin: empty key
	case "random":
		return strings.Repeat("x", rand.Intn(10))
	default:
		return ev.PageID
	}
}

func (p *Producer) sendSync(ctx context.Context, msg kafka.Message, start time.Time) error {
	err := p.writer.WriteMessages(ctx, msg)
	latency := time.Since(start)
	if err != nil {
		p.metrics.RecordFailed()
		log.Printf("kafka send failed: %v", err)
		return err
	}
	p.metrics.RecordSent(latency)
	return nil
}

func (p *Producer) sendAsync(ctx context.Context, msg kafka.Message, start time.Time) error {
	// Async mode: write and handle callback
	errChan := make(chan error, 1)
	go func() {
		err := p.writer.WriteMessages(ctx, msg)
		latency := time.Since(start)
		if err != nil {
			p.metrics.RecordFailed()
			log.Printf("kafka async send failed: %v", err)
			errChan <- err
		} else {
			p.metrics.RecordSent(latency)
			log.Printf("kafka async send success latency=%v", latency)
			errChan <- nil
		}
	}()
	
	// Non-blocking: return immediately
	select {
	case err := <-errChan:
		return err
	case <-time.After(100 * time.Millisecond):
		// Assume success for async
		return nil
	}
}

func (p *Producer) sendBatch(ctx context.Context, msg kafka.Message, start time.Time) error {
	p.batchMu.Lock()
	p.batch = append(p.batch, msg)
	shouldFlush := len(p.batch) >= p.batchSize
	p.batchMu.Unlock()

	if shouldFlush {
		return p.Flush(ctx)
	}
	return nil
}

func (p *Producer) Flush(ctx context.Context) error {
	p.batchMu.Lock()
	if len(p.batch) == 0 {
		p.batchMu.Unlock()
		return nil
	}
	batch := make([]kafka.Message, len(p.batch))
	copy(batch, p.batch)
	p.batch = p.batch[:0]
	p.batchMu.Unlock()

	start := time.Now()
	err := p.writer.WriteMessages(ctx, batch...)
	latency := time.Since(start)
	if err != nil {
		p.metrics.RecordFailed()
		log.Printf("kafka batch flush failed: %v", err)
		return err
	}
	p.metrics.RecordSent(latency)
	log.Printf("kafka batch flushed %d messages latency=%v", len(batch), latency)
	return nil
}

func (p *Producer) UpdateConfig(sendMode string, batchSize int, flushMs int, strategy string) {
	p.batchMu.Lock()
	defer p.batchMu.Unlock()
	
	p.sendMode = sendMode
	p.batchSize = batchSize
	p.flushInterval = time.Duration(flushMs) * time.Millisecond
	p.partitionStrategy = strategy

	if p.flushTicker != nil {
		p.flushTicker.Stop()
	}
	if sendMode == "batch" {
		p.flushTicker = time.NewTicker(p.flushInterval)
		go p.flushLoop()
	}
}

func (p *Producer) GetMetrics() *Metrics {
	return p.metrics
}

func (p *Producer) Close() error {
	if p.flushTicker != nil {
		p.flushTicker.Stop()
		close(p.stopFlush)
	}
	// Final flush
	p.Flush(context.Background())
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
