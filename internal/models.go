package internal

import "github.com/google/uuid"

type OperationType string

const (
	Deposit  OperationType = "DEPOSIT"
	Withdraw OperationType = "WITHDRAW"
)

type OperationRequest struct {
	WalletID       uuid.UUID     `json:"walletId"`
	OperationType  OperationType `json:"operationType"`
	Amount         int64         `json:"amount"`
}

type BalanceResponse struct {
	WalletID uuid.UUID `json:"walletId"`
	Balance  int64     `json:"balance"`
}
