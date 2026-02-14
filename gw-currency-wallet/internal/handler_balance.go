package internal

import (
	"encoding/json"
	"log"
	"net/http"

	"wall/internal/http/middleware"
)

var allowedCurrencies = map[string]bool{"USD": true, "RUB": true, "EUR": true}

type BalanceResponseV1 struct {
	Balance map[string]float64 `json:"balance"`
}

type AmountCurrencyRequest struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

func (h *Handler) HandleBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
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

	balances, err := h.authRepo.GetBalances(r.Context(), walletID)
	if err != nil {
		log.Printf("get balances: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, BalanceResponseV1{Balance: balances})
}

func (h *Handler) HandleDeposit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	var req AmountCurrencyRequest
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
	if !allowedCurrencies[req.Currency] {
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

	newAmount, err := h.authRepo.Deposit(r.Context(), walletID, req.Currency, req.Amount)
	if err != nil {
		if err == ErrNotFound {
			writeError(w, http.StatusNotFound, "currency not found")
			return
		}
		log.Printf("deposit: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	log.Printf("INFO: user_id=%d currency=%s amount=%.2f type=deposit", userID, req.Currency, req.Amount)
	writeJSON(w, http.StatusOK, map[string]any{"currency": req.Currency, "amount": newAmount})
}

func (h *Handler) HandleWithdraw(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	var req AmountCurrencyRequest
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
	if !allowedCurrencies[req.Currency] {
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

	newAmount, err := h.authRepo.Withdraw(r.Context(), walletID, req.Currency, req.Amount)
	if err != nil {
		if err == ErrInsufficientFunds {
			writeError(w, http.StatusBadRequest, "Insufficient funds")
			return
		}
		if err == ErrNotFound {
			writeError(w, http.StatusNotFound, "currency not found")
			return
		}
		log.Printf("withdraw: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	log.Printf("INFO: user_id=%d currency=%s amount=%.2f type=withdraw", userID, req.Currency, req.Amount)
	writeJSON(w, http.StatusOK, map[string]any{"currency": req.Currency, "amount": newAmount})
}
