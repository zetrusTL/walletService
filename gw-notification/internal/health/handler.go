package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"

	"notification/internal/config"
)

// Response — структура ответа GET /health.
type Response struct {
	Status string `json:"status"`
	Mongo  string `json:"mongo"`
	Kafka  string `json:"kafka"`
}

// Handler обрабатывает GET /health.
type Handler struct {
	mongoClient *mongo.Client
	cfg         *config.Config
}

// NewHandler создаёт health handler.
func NewHandler(mongoClient *mongo.Client, cfg *config.Config) *Handler {
	return &Handler{mongoClient: mongoClient, cfg: cfg}
}

// ServeHTTP проверяет Mongo и Kafka.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/health" {
		http.NotFound(w, r)
		return
	}
	resp := Response{
		Status: "ok",
		Mongo:  "ok",
		Kafka:  "ok",
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Mongo ping
	if h.mongoClient != nil {
		if err := h.mongoClient.Ping(ctx, nil); err != nil {
			resp.Status = "degraded"
			resp.Mongo = "error"
		}
	}

	// Kafka — подключение к первому брокеру
	brokers := h.cfg.KafkaBrokerList()
	if len(brokers) > 0 {
		dialer := &kafka.Dialer{Timeout: 3 * time.Second}
		conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
		if err != nil {
			resp.Status = "degraded"
			resp.Kafka = "error"
		} else {
			_ = conn.Close()
		}
	} else {
		// Kafka не настроен — считаем ok
		resp.Kafka = "ok"
	}

	status := http.StatusOK
	if resp.Status == "degraded" {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}
