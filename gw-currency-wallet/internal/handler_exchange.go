package internal

import (
	"encoding/json"
	"log"
	"math"
	"net/http"

	"wall/internal/http/middleware"
)

// ExchangeRatesResponse is the JSON response for GET /api/v1/exchange/rates.
type ExchangeRatesResponse struct {
	Rates  map[string]float64 `json:"rates"`
	Source string             `json:"source"`
}

// ExchangeRequest is the body for POST /api/v1/exchange.
type ExchangeRequest struct {
	FromCurrency string  `json:"from_currency"`
	ToCurrency   string  `json:"to_currency"`
	Amount       float64 `json:"amount"`
}

// ExchangeResponse is the 200 response for POST /api/v1/exchange.
type ExchangeResponse struct {
	Message         string             `json:"message"`
	Rate            float64            `json:"rate"`
	ExchangedAmount float64            `json:"exchanged_amount"`
	NewBalance      map[string]float64 `json:"new_balance"`
	Source          string             `json:"source"`
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

func (h *Handler) HandleExchange(w http.ResponseWriter, r *http.Request) { 
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	var req ExchangeRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, ErrInvalidAmount.Error())
		return
	}
	if req.FromCurrency == req.ToCurrency {
		writeError(w, http.StatusBadRequest, "from_currency and to_currency must differ")
		return
	}
	if !allowedCurrencies[req.FromCurrency] || !allowedCurrencies[req.ToCurrency] {
		writeError(w, http.StatusBadRequest, ErrInvalidCurrency.Error())
		return
	}

	walletID, err := h.authRepo.GetWalletIDByUserID(r.Context(), userID) 
	if err != nil {
		if err == ErrNotFound {
			writeError(w, http.StatusNotFound, "wallet not found")
			return
		}
		log.Printf("get wallet: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	var rate float64
	var source string
	if h.exchangerClient != nil {
		rate, source, err = h.exchangerClient.GetRateWithSource(r.Context(), req.FromCurrency, req.ToCurrency)
	} else {
		writeError(w, http.StatusServiceUnavailable, "exchange rate service unavailable")
		return
	}
	if err != nil {
		log.Printf("exchange rate: %v", err)
		writeError(w, http.StatusBadRequest, "exchange rate not available for "+req.FromCurrency+"_"+req.ToCurrency)
		return
	}

	exchangedAmount := math.Round(req.Amount*rate*100) / 100

	newBalance, err := h.authRepo.Exchange(r.Context(), walletID, req.FromCurrency, req.ToCurrency, req.Amount, exchangedAmount)
	if err != nil {
		if err == ErrInsufficientFunds {
			writeError(w, http.StatusBadRequest, "insufficient funds")
			return
		}
		log.Printf("exchange: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if h.largeTxPublisher != nil {
		h.largeTxPublisher.Publish(r.Context(), userID, "exchange", req.FromCurrency, req.Amount)
	}
	log.Printf("INFO: user_id=%d from=%s to=%s amount=%.2f rate=%.2f exchanged_amount=%.2f source=%s", 
		userID, req.FromCurrency, req.ToCurrency, req.Amount, rate, exchangedAmount, source) 

	writeJSON(w, http.StatusOK, ExchangeResponse{ 
		Message:         "Exchange successful",
		Rate:            rate,
		ExchangedAmount: exchangedAmount,
		NewBalance:      newBalance,
		Source:          source,
	})
}
