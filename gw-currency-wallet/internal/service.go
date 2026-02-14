package internal

import (
	"context"

	"github.com/google/uuid"
)

type WalletService struct {
	repo WalletRepository
}

func NewWalletService(repo WalletRepository) *WalletService {
	return &WalletService{repo: repo}
}

func (s *WalletService) Apply(ctx context.Context, walletID uuid.UUID, op OperationType, amount int64) (int64, error) {
	if amount <= 0 {
		return 0, ErrInvalidAmount
	}

	if op != Deposit && op != Withdraw {
		return 0, ErrInvalidOperation
	}

	return s.repo.ApplyOperation(ctx, walletID, op, amount)
}

func (s *WalletService) GetBalance(ctx context.Context, walletID uuid.UUID) (int64, error) {
	return s.repo.GetBalance(ctx, walletID)
}
