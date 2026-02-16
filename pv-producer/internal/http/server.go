package http

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"pv-producer/internal/config"
	"pv-producer/internal/generator"
	"pv-producer/internal/kafka"
)

type ControlRequest struct {
	Mode          string `json:"mode"`           // regular, burst, night
	RPS           int    `json:"rps"`
	BatchSize     int    `json:"batch_size"`
	FlushMs       int    `json:"flush_ms"`
	Strategy      string `json:"strategy"`       // key, rr, random
	SendMode      string `json:"send_mode"`      // sync, async, batch
}

type Server struct {
	cfg       *config.Config
	generator *generator.Generator
	producer  *kafka.Producer
}

func NewServer(cfg *config.Config, gen *generator.Generator, prod *kafka.Producer) *Server {
	return &Server{
		cfg:       cfg,
		generator: gen,
		producer:  prod,
	}
}

func (s *Server) HandleControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Update generator
	if req.Mode != "" {
		s.generator.SetMode(req.Mode)
	}
	if req.RPS > 0 {
		s.generator.SetRPS(req.RPS)
	}

	// Update producer
	if req.SendMode != "" || req.BatchSize > 0 || req.FlushMs > 0 || req.Strategy != "" {
		sendMode := req.SendMode
		if sendMode == "" {
			sendMode = s.cfg.SendMode
		}
		batchSize := req.BatchSize
		if batchSize == 0 {
			batchSize = s.cfg.BatchSize
		}
		flushMs := req.FlushMs
		if flushMs == 0 {
			flushMs = s.cfg.FlushIntervalMs
		}
		strategy := req.Strategy
		if strategy == "" {
			strategy = s.cfg.PartitionStrategy
		}
		s.producer.UpdateConfig(sendMode, batchSize, flushMs, strategy)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"config": map[string]interface{}{
			"mode":       req.Mode,
			"rps":        req.RPS,
			"batch_size": req.BatchSize,
			"flush_ms":   req.FlushMs,
			"strategy":   req.Strategy,
			"send_mode":  req.SendMode,
		},
	})
}

func (s *Server) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metrics := s.producer.GetMetrics()
	sent, failed, avgLatency, p95Latency := metrics.GetStats()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"sent":        sent,
		"failed":      failed,
		"avg_latency_ms": avgLatency.Milliseconds(),
		"p95_latency_ms": p95Latency.Milliseconds(),
		"events_generated": s.generator.GetEventCount(),
	})
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/control", s.HandleControl)
	mux.HandleFunc("/metrics", s.HandleMetrics)

	server := &http.Server{
		Addr:         ":" + s.cfg.HTTPPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Printf("pv-producer HTTP server starting on :%s", s.cfg.HTTPPort)
	return server.ListenAndServe()
}
