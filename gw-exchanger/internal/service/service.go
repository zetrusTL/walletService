package service

import (
	"context"

	"exchanger/internal/storage/postgres"
)

// RateMapKey is the format for GetExchangeRates map keys: "FROM_TO" (e.g. "USD_RUB", "EUR_USD").
const RateMapKey = "FROM_TO"

type Service struct {
	repo *postgres.RatesRepo
}

func New(repo *postgres.RatesRepo) *Service {
	return &Service{repo: repo}
}

// GetExchangeRates returns all rates. Map key format: "FROM_TO" (e.g. "USD_RUB", "EUR_USD").
func (s *Service) GetExchangeRates(ctx context.Context) (map[string]float64, error) {
	return s.repo.AllRates(ctx)
}

// GetExchangeRateForCurrency returns rate for the given pair. Returns (0, false, nil) if not found.
func (s *Service) GetExchangeRateForCurrency(ctx context.Context, fromCurrency, toCurrency string) (rate float64, found bool, err error) {
	return s.repo.Rate(ctx, fromCurrency, toCurrency)
}
