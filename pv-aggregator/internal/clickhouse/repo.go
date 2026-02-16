package clickhouse

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"pv-aggregator/internal/model"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type PageViewRow struct {
	EventDate      time.Time
	EventTime      time.Time
	PageID         string
	UserID         string
	DurationMs     uint32
	UserAgent      string
	IPAddress      net.IP
	Region         string
	IsBounce       uint8
	KafkaOffset    int64
	KafkaPartition int32
	ProcessedTime  time.Time
}

type Repo struct {
	conn driver.Conn
}

func NewRepo(client *Client) *Repo {
	return &Repo{conn: client.Conn()}
}

func (r *Repo) InsertRawBatch(ctx context.Context, rows []PageViewRow) error {
	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO page_views_raw")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, row := range rows {
		if err := batch.Append(
			row.EventDate,
			row.EventTime,
			row.PageID,
			row.UserID,
			row.DurationMs,
			row.UserAgent,
			row.IPAddress,
			row.Region,
			row.IsBounce,
			row.KafkaOffset,
			row.KafkaPartition,
			row.ProcessedTime,
		); err != nil {
			return fmt.Errorf("append row: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	log.Printf("inserted %d rows into page_views_raw", len(rows))
	return nil
}

func (r *Repo) InsertError(ctx context.Context, rawMessage string, errorReason string, kafkaOffset int64, kafkaPartition int32) error {
	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO processing_errors")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	if err := batch.Append(
		time.Now(),
		rawMessage,
		errorReason,
		kafkaOffset,
		kafkaPartition,
	); err != nil {
		return fmt.Errorf("append error row: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send error batch: %w", err)
	}

	return nil
}

func ConvertEventToRow(ev *model.PageViewEvent, kafkaOffset int64, kafkaPartition int32) PageViewRow {
	now := time.Now()
	eventTime := ev.Timestamp
	if eventTime.IsZero() {
		eventTime = now
	}

	// Parse IP address
	ip := net.ParseIP(ev.IPAddress)
	if ip == nil {
		// Use IPv6 zero address if parsing fails
		ip = net.IPv6zero
	}
	// Convert to IPv6 if IPv4
	if ip.To4() != nil {
		ip = ip.To16()
	}

	// Convert bounce to UInt8
	isBounce := uint8(0)
	if ev.IsBounce || ev.ViewDuration < 5000 {
		isBounce = 1
	}

	return PageViewRow{
		EventDate:      eventTime.UTC().Truncate(24 * time.Hour),
		EventTime:      eventTime.UTC(),
		PageID:         ev.PageID,
		UserID:         ev.UserID,
		DurationMs:     uint32(ev.ViewDuration),
		UserAgent:      ev.UserAgent,
		IPAddress:      ip,
		Region:         ev.Region,
		IsBounce:       isBounce,
		KafkaOffset:    kafkaOffset,
		KafkaPartition: kafkaPartition,
		ProcessedTime:  now,
	}
}
