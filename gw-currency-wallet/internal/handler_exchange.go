package internal

import (
	"log"
	"net/http"
)

// ExchangeRatesResponse is the JSON response for GET /api/v1/exchange/rates.
type ExchangeRatesResponse struct {
	Rates  map[string]float64 `json:"rates"`
	Source string             `json:"source"`
}

func (h *Handler) HandleExchangeRates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var rates map[string]float64
	var source string
	var err error
	if h.exchangerClient != nil {
		rates, source, err = h.exchangerClient.GetAllRates(r.Context())
	} else {
		rates = map[string]float64{}
		source = "grpc"
	}
	if err != nil {
		log.Printf("exchange rates: %v", err)
		writeError(w, http.StatusBadGateway, "exchange service unavailable")
		return
	}
	if rates == nil {
		rates = map[string]float64{}
	}
	writeJSON(w, http.StatusOK, ExchangeRatesResponse{Rates: rates, Source: source})
}
