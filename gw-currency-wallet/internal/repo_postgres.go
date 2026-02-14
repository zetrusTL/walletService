package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{pool: pool}
}

func (r *PostgresRepo) GetBalance(ctx context.Context, walletID uuid.UUID) (int64, error) {
	var balance int64
	err := r.pool.QueryRow(ctx,
		`SELECT balance FROM wallet_legacy WHERE id = $1`,
		walletID,
	).Scan(&balance)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("get balance: %w", err)
	}
	return balance, nil
}

func (r *PostgresRepo) ApplyOperation(ctx context.Context, walletID uuid.UUID, op OperationType, amount int64) (int64, error) {
	switch op {
	case Deposit:
		return r.deposit(ctx, walletID, amount)
	case Withdraw:
		return r.withdraw(ctx, walletID, amount)
	default:
		return 0, ErrInvalidOperation
	}
}

func (r *PostgresRepo) deposit(ctx context.Context, walletID uuid.UUID, amount int64) (int64, error) {
	var newBalance int64
	err := r.pool.QueryRow(ctx, `
		UPDATE wallet_legacy
		SET balance = balance + $1
		WHERE id = $2
		RETURNING balance
	`, amount, walletID).Scan(&newBalance)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("deposit: %w", err)
	}
	return newBalance, nil
}

func (r *PostgresRepo) withdraw(ctx context.Context, walletID uuid.UUID, amount int64) (int64, error) {
	var newBalance int64
	err := r.pool.QueryRow(ctx, `
		UPDATE wallet_legacy
		SET balance = balance - $1
		WHERE id = $2 AND balance >= $1
		RETURNING balance
	`, amount, walletID).Scan(&newBalance)

	if err == nil {
		return newBalance, nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		exErr := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_legacy WHERE id = $1)`, walletID).Scan(&exists)
		if exErr != nil {
			return 0, fmt.Errorf("withdraw exists-check: %w", exErr)
		}
		if !exists {
			return 0, ErrNotFound
		}
		return 0, ErrInsufficientFunds
	}

	return 0, fmt.Errorf("withdraw: %w", err)
}
