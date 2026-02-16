package generator

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"pv-producer/internal/model"
	"github.com/google/uuid"
)

type Generator struct {
	mode          string // regular, burst, night
	rps           int
	duplicateRate float64 // 0.01 = 1%
	lastEvent     *model.PageViewEvent
	lastEventID   string
	eventCounter  int64
}

func NewGenerator(mode string, rps int) *Generator {
	return &Generator{
		mode:         mode,
		rps:          rps,
		duplicateRate: 0.01, // 1% duplicates
		eventCounter: 0,
	}
}

func (g *Generator) Generate() *model.PageViewEvent {
	atomic.AddInt64(&g.eventCounter, 1)
	
	// 1% duplicates: reuse same event_id and payload
	if rand.Float64() < g.duplicateRate && g.lastEvent != nil {
		ev := *g.lastEvent
		ev.EventID = g.lastEventID
		return &ev
	}

	ev := g.generateNew()
	g.lastEvent = ev
	g.lastEventID = ev.EventID
	return ev
}

func (g *Generator) generateNew() *model.PageViewEvent {
	ev := &model.PageViewEvent{
		EventID:   uuid.New().String(),
		PageID:    g.generatePageID(),
		UserID:    uuid.New().String()[:8],
		Timestamp: time.Now().UTC(),
		UserAgent: g.randomUserAgent(),
		IPAddress: g.randomIP(),
		Region:    g.randomRegion(),
	}

	// 5% error events
	if rand.Float64() < 0.05 {
		return g.generateErrorEvent(ev)
	}

	// 10% bounce events
	isBounce := rand.Float64() < 0.10
	if isBounce {
		ev.ViewDuration = rand.Intn(5000) // < 5 seconds
		ev.IsBounce = true
	} else {
		ev.ViewDuration = 10000 + rand.Intn(590000) // 10-600 seconds
		ev.IsBounce = false
	}

	return ev
}

func (g *Generator) generateErrorEvent(base *model.PageViewEvent) *model.PageViewEvent {
	errorType := rand.Intn(3)
	switch errorType {
	case 0:
		// Empty page_id
		base.PageID = ""
	case 1:
		// Negative duration
		base.ViewDuration = -rand.Intn(1000)
	case 2:
		// Valid event but will be corrupted in JSON later
		// (handled in producer)
	}
	return base
}

func (g *Generator) generatePageID() string {
	pages := []string{
		"/home", "/products", "/about", "/contact", "/cart",
		"/checkout", "/profile", "/settings", "/help", "/search",
	}
	return pages[rand.Intn(len(pages))]
}

func (g *Generator) randomUserAgent() string {
	agents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
	}
	return agents[rand.Intn(len(agents))]
}

func (g *Generator) randomIP() string {
	return fmt.Sprintf("2001:db8::%x:%x:%x:%x",
		rand.Intn(65536), rand.Intn(65536),
		rand.Intn(65536), rand.Intn(65536))
}

func (g *Generator) randomRegion() string {
	regions := []string{"US", "EU", "ASIA", "LATAM", "AFRICA"}
	return regions[rand.Intn(len(regions))]
}

func (g *Generator) GetInterval() time.Duration {
	switch g.mode {
	case "regular":
		// 1-10 events/sec
		if g.rps <= 0 {
			g.rps = 5
		}
		intervalMs := 1000 / g.rps
		if intervalMs < 100 {
			intervalMs = 100 // min 100ms = max 10/sec
		}
		return time.Duration(intervalMs) * time.Millisecond
	case "burst":
		// Burst mode: wait, then generate many
		return 2 * time.Second // Will be overridden by burst logic
	case "night":
		// 1 event per 10 seconds
		return 10 * time.Second
	default:
		return time.Second
	}
}

func (g *Generator) SetMode(mode string) {
	g.mode = mode
}

func (g *Generator) GetMode() string {
	return g.mode
}

func (g *Generator) SetRPS(rps int) {
	g.rps = rps
}

func (g *Generator) GetEventCount() int64 {
	return atomic.LoadInt64(&g.eventCounter)
}
