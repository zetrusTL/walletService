package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/pgconn"
)

var supportedCurrencies = []string{"USD", "RUB", "EUR"}

type AuthRepo struct {
	pool *pgxpool.Pool
}

func NewAuthRepo(pool *pgxpool.Pool) *AuthRepo {
	return &AuthRepo{pool: pool}
}

func (r *AuthRepo) CreateUser(ctx context.Context, username, email, passwordHash string) (userID int64, err error) {
	err = r.pool.QueryRow(ctx,
		`INSERT INTO users (username, email, password_hash) 
		VALUES ($1, $2, $3) RETURNING id`,
		username, email, passwordHash,
	).Scan(&userID)
	if err != nil {
		if isUniqueViolation(err) {
			return 0, ErrUserExists
		}
		return 0, fmt.Errorf("create user: %w", err)
	}
	return userID, nil
}

func (r *AuthRepo) GetUserByUsername(ctx context.Context, username string) (id int64, email, passwordHash string, err error) {
	err = r.pool.QueryRow(ctx,
		`SELECT id, email, password_hash FROM users WHERE username = $1`,
		username,
	).Scan(&id, &email, &passwordHash)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, "", "", ErrAuthFailed
		}
		return 0, "", "", fmt.Errorf("get user: %w", err)
	}
	return id, email, passwordHash, nil
}

func (r *AuthRepo) CreateWalletForUser(ctx context.Context, userID int64) (walletID int64, err error) {
	err = r.pool.QueryRow(ctx,
		`INSERT INTO wallets (user_id) VALUES ($1) RETURNING id`,
		userID,
	).Scan(&walletID)
	if err != nil {
		return 0, fmt.Errorf("create wallet: %w", err)
	}
	return walletID, nil
}

func (r *AuthRepo) CreateBalancesForWallet(ctx context.Context, walletID int64) error {
	for _, c := range supportedCurrencies {
		_, err := r.pool.Exec(ctx,
			`INSERT INTO balances (wallet_id, currency, amount) VALUES ($1, $2, 0)`,
			walletID, c,
		)
		if err != nil {
			return fmt.Errorf("create balance %s: %w", c, err)
		}
	}
	return nil
}

func (r *AuthRepo) GetWalletIDByUserID(ctx context.Context, userID int64) (int64, error) {
	var walletID int64
	err := r.pool.QueryRow(ctx,
		`SELECT id FROM wallets WHERE user_id = $1`,
		userID,
	).Scan(&walletID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("get wallet: %w", err)
	}
	return walletID, nil
}

func (r *AuthRepo) GetBalances(ctx context.Context, walletID int64) (map[string]float64, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT currency, amount 
		FROM balances WHERE wallet_id = $1`,
		walletID,
	)
	if err != nil {
		return nil, fmt.Errorf("get balances: %w", err)
	}
	defer rows.Close()
	out := make(map[string]float64)
	for _, c := range supportedCurrencies {
		out[c] = 0
	}
	for rows.Next() {
		var currency string
		var amount float64
		if err := rows.Scan(&currency, &amount); err != nil {
			return nil, fmt.Errorf("scan balance: %w", err)
		}
		out[currency] = amount
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *AuthRepo) Deposit(ctx context.Context, walletID int64, currency string, amount float64) (newAmount float64, err error) {
	err = r.pool.QueryRow(ctx,
		`UPDATE balances 
		SET amount = amount + $1 
		WHERE wallet_id = $2 AND currency = $3 
		RETURNING amount`,
		amount, walletID, currency,
	).Scan(&newAmount)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("deposit: %w", err)
	}
	return newAmount, nil
}

func (r *AuthRepo) Withdraw(ctx context.Context, walletID int64, currency string, amount float64) (newAmount float64, err error) {
	err = r.pool.QueryRow(ctx,
		`UPDATE balances 
		SET amount = amount - $1 
		WHERE wallet_id = $2 AND currency = $3 AND amount >= $1 
		RETURNING amount`,
		amount, walletID, currency,
	).Scan(&newAmount)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, ErrInsufficientFunds
		}
		return 0, fmt.Errorf("withdraw: %w", err)
	}
	return newAmount, nil
}

func (r *AuthRepo) Exchange(ctx context.Context, walletID int64, fromCurrency, toCurrency string, debitAmount, creditAmount float64) (newBalance map[string]float64, err error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cmd, err := tx.Exec(ctx,
		`UPDATE balances
		 SET amount = amount - $1 
		 WHERE wallet_id = $2 AND currency = $3 AND amount >= $1`,
		debitAmount, walletID, fromCurrency,
	)
	if err != nil {
		return nil, fmt.Errorf("exchange debit: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return nil, ErrInsufficientFunds
	}

	_, err = tx.Exec(ctx,
		`UPDATE balances 
		SET amount = amount + $1 
		WHERE wallet_id = $2 AND currency = $3`,
		creditAmount, walletID, toCurrency,
	)
	if err != nil {
		return nil, fmt.Errorf("exchange credit: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return r.GetBalances(ctx, walletID)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}
