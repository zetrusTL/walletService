package agg

import (
	"sync"
	"time"

	"pv-aggregator/internal/clickhouse"
	"github.com/segmentio/kafka-go"
)

type BufferedMessage struct {
	Message *kafka.Message
	Row     clickhouse.PageViewRow
}

type Buffer struct {
	mu          sync.Mutex
	messages    []BufferedMessage
	maxSize     int
	flushTicker *time.Ticker
	flushChan   chan struct{}
}

func NewBuffer(maxSize int, flushInterval time.Duration) *Buffer {
	b := &Buffer{
		messages:  make([]BufferedMessage, 0, maxSize),
		maxSize:   maxSize,
		flushChan: make(chan struct{}, 1),
	}
	if flushInterval > 0 {
		b.flushTicker = time.NewTicker(flushInterval)
	}
	return b
}

func (b *Buffer) Add(msg BufferedMessage) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	b.messages = append(b.messages, msg)
	shouldFlush := len(b.messages) >= b.maxSize
	
	if shouldFlush {
		select {
		case b.flushChan <- struct{}{}:
		default:
		}
	}
	
	return shouldFlush
}

func (b *Buffer) GetAndClear() []BufferedMessage {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if len(b.messages) == 0 {
		return nil
	}
	
	result := make([]BufferedMessage, len(b.messages))
	copy(result, b.messages)
	b.messages = b.messages[:0]
	return result
}

// PutBack prepends failed messages for retry on next flush. Used when ClickHouse insert fails.
func (b *Buffer) PutBack(messages []BufferedMessage) {
	if len(messages) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages = append(messages, b.messages...)
}

func (b *Buffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.messages)
}

func (b *Buffer) FlushTicker() *time.Ticker {
	return b.flushTicker
}

func (b *Buffer) FlushChan() <-chan struct{} {
	return b.flushChan
}

func (b *Buffer) Stop() {
	if b.flushTicker != nil {
		b.flushTicker.Stop()
	}
}
