package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

type mockRepo struct {
	lastWalletID uuid.UUID
	lastOp       OperationType
	lastAmount   int64
	balance      int64
	err          error
}

func (m *mockRepo) ApplyOperation(ctx context.Context, walletID uuid.UUID, op OperationType, amount int64) (int64, error) {
	m.lastWalletID = walletID
	m.lastOp = op
	m.lastAmount = amount
	return m.balance, m.err
}

func (m *mockRepo) GetBalance(ctx context.Context, walletID uuid.UUID) (int64, error) {
	return m.balance, m.err
}

func TestService_ValidateAmount(t *testing.T) {
	repo := &mockRepo{}
	svc := NewWalletService(repo)

	_, err := svc.Apply(context.Background(), uuid.New(), Deposit, 0)
	if err != ErrInvalidAmount {
		t.Fatalf("expected ErrInvalidAmount, got %v", err)
	}
}

func TestService_ValidateOperation(t *testing.T) {
	repo := &mockRepo{}
	svc := NewWalletService(repo)

	_, err := svc.Apply(context.Background(), uuid.New(), OperationType("BAD"), 10)
	if err != ErrInvalidOperation {
		t.Fatalf("expected ErrInvalidOperation, got %v", err)
	}
}

func TestService_CallsRepo(t *testing.T) {
	repo := &mockRepo{balance: 123}
	svc := NewWalletService(repo)

	id := uuid.New()
	got, err := svc.Apply(context.Background(), id, Deposit, 10)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != 123 {
		t.Fatalf("expected balance 123, got %d", got)
	}
	if repo.lastWalletID != id || repo.lastOp != Deposit || repo.lastAmount != 10 {
		t.Fatalf("repo wasn't called with expected args")
	}
}
