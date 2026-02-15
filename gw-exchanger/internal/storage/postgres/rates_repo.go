package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RatesRepo struct {
	pool *pgxpool.Pool
}

func NewRatesRepo(pool *pgxpool.Pool) *RatesRepo {
	return &RatesRepo{pool: pool}
}

// AllRates returns all rows as map key "FROM_TO" (e.g. "USD_RUB") -> rate.
func (r *RatesRepo) AllRates(ctx context.Context) (map[string]float64, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT from_currency, to_currency, rate FROM currency_rates`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]float64)
	for rows.Next() {
		var from, to string
		var rate float64
		if err := rows.Scan(&from, &to, &rate); err != nil {
			return nil, err
		}
		out[from+"_"+to] = rate
	}
	return out, rows.Err()
}

// Rate returns rate for from_currency -> to_currency. ok is false if not found.
func (r *RatesRepo) Rate(ctx context.Context, fromCurrency, toCurrency string) (rate float64, ok bool, err error) {
	err = r.pool.QueryRow(ctx,
		`SELECT rate FROM currency_rates WHERE from_currency = $1 AND to_currency = $2`,
		fromCurrency, toCurrency).Scan(&rate)
	if err != nil {
		if isNoRows(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return rate, true, nil
}

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
