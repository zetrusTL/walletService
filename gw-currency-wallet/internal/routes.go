package internal

import (
	"net/http"

	"wall/internal/http/middleware"
)

func RegisterRoutes(mux *http.ServeMux, h *Handler, jwtSecret []byte) {
	authMW := middleware.AuthMiddleware(jwtSecret)

	// Health (no auth)
	mux.HandleFunc("/health", h.HandleHealth)

	// Auth (no middleware)
	mux.HandleFunc("/api/v1/register", h.HandleRegister)
	mux.HandleFunc("/api/v1/login", h.HandleLogin)

	// Protected: multi-currency balance (JWT required)
	mux.Handle("/api/v1/balance", authMW(http.HandlerFunc(h.HandleBalance)))
	mux.Handle("/api/v1/wallet/deposit", authMW(http.HandlerFunc(h.HandleDeposit)))
	mux.Handle("/api/v1/wallet/withdraw", authMW(http.HandlerFunc(h.HandleWithdraw)))
	mux.Handle("/api/v1/exchange", authMW(http.HandlerFunc(h.HandleExchange)))
	mux.Handle("/api/v1/exchange/rates", authMW(http.HandlerFunc(h.HandleExchangeRates)))
}
