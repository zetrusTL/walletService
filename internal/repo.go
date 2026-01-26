package internal

import (
	"context"

	"github.com/google/uuid"
)

type WalletRepository interface {
	ApplyOperation(ctx context.Context, walletID uuid.UUID, op OperationType, amount int64) (newBalance int64, err error)
	GetBalance(ctx context.Context, walletID uuid.UUID) (balance int64, err error)
}
