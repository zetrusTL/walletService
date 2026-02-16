package http

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"pv-aggregator/internal/config"
	"pv-aggregator/internal/kafka"
)

type Server struct {
	cfg      *config.Config
	consumer *kafka.Consumer
}

func NewServer(cfg *config.Config, consumer *kafka.Consumer) *Server {
	return &Server{
		cfg:      cfg,
		consumer: consumer,
	}
}

func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Basic health check
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
	})
}

func (s *Server) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metrics := s.consumer.GetMetrics()
	consumed, inserted, dlq, errors, lastFlush := metrics.GetStats()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"consumed":      consumed,
		"inserted_raw":   inserted,
		"dlq_count":      dlq,
		"ch_errors":      errors,
		"last_flush_time": lastFlush.Format(time.RFC3339),
	})
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.HandleHealth)
	mux.HandleFunc("/metrics", s.HandleMetrics)

	server := &http.Server{
		Addr:         ":" + s.cfg.HealthPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	log.Printf("pv-aggregator HTTP server starting on :%s", s.cfg.HealthPort)
	return server.ListenAndServe()
}
