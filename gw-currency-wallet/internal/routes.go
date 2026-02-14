package internal

import (
	"net/http"
	"strings"

	"github.com/google/uuid"
)

func RegisterRoutes(mux *http.ServeMux, h *Handler) {
	// POST /api/v1/wallet
	mux.HandleFunc("/api/v1/wallet", h.HandleWalletOperation)

	// GET /api/v1/wallets/{uuid}
	mux.HandleFunc("/api/v1/wallets/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) != 4 {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		idStr := parts[3]
		walletID, err := uuid.Parse(idStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid wallet uuid")
			return
		}

		h.HandleGetBalance(w, r, walletID)
	})
}
