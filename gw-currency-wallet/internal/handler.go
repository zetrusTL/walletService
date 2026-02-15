package internal

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/google/uuid"
	"wall/internal/grpcclient"
)

type Handler struct {
	svc           *WalletService
	authRepo      *AuthRepo
	jwtSecret     []byte
	exchangerClient *grpcclient.ExchangerClient
}

func NewHandler(svc *WalletService, authRepo *AuthRepo, jwtSecret []byte, exchangerClient *grpcclient.ExchangerClient) *Handler {
	return &Handler{svc: svc, authRepo: authRepo, jwtSecret: jwtSecret, exchangerClient: exchangerClient}
}

func (h *Handler) HandleWalletOperation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req OperationRequest											
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() 
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.WalletID == uuid.Nil {
		writeError(w, http.StatusBadRequest, "walletId is required")
		return
	}

	newBalance, err := h.svc.Apply(r.Context(), req.WalletID, req.OperationType, req.Amount)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, BalanceResponse{
		WalletID: req.WalletID,
		Balance:  newBalance,
	})
}

func (h *Handler) HandleGetBalance(w http.ResponseWriter, r *http.Request, walletID uuid.UUID) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	balance, err := h.svc.GetBalance(r.Context(), walletID)
	if err != nil {
		writeDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, BalanceResponse{
		WalletID: walletID,
		Balance:  balance,
	})
}

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalidAmount), errors.Is(err, ErrInvalidOperation):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrInsufficientFunds):
		writeError(w, http.StatusConflict, err.Error()) // 409
	default:
		log.Printf("unexpected error: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
