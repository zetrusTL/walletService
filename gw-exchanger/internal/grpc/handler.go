package grpchandler

import (
	"context"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"exchanger/internal/service"
	"exchanger/internal/exchange"
)

type Handler struct {
	exchange.UnimplementedExchangeServiceServer
	svc *service.Service
}

func New(svc *service.Service) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) GetExchangeRates(ctx context.Context, _ *exchange.Empty) (*exchange.ExchangeRatesResponse, error) {
	rates, err := h.svc.GetExchangeRates(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	log.Printf("GetExchangeRates: returning %d rates", len(rates))
	return &exchange.ExchangeRatesResponse{Rates: rates}, nil
}

func (h *Handler) GetExchangeRateForCurrency(ctx context.Context, req *exchange.CurrencyRequest) (*exchange.ExchangeRateResponse, error) {
	from, to := req.GetFromCurrency(), req.GetToCurrency()
	rate, found, err := h.svc.GetExchangeRateForCurrency(ctx, from, to)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !found {
		log.Printf("GetExchangeRateForCurrency: from=%s to=%s not found", from, to)
		return nil, status.Error(codes.NotFound, "currency pair not found")
	}
	log.Printf("GetExchangeRateForCurrency: from=%s to=%s rate=%.4f", from, to, rate)
	return &exchange.ExchangeRateResponse{
		FromCurrency: from,
		ToCurrency:   to,
		Rate:         rate,
	}, nil
}
