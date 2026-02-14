package internal

import (
	"errors"
)

var (
	ErrNotFound          = errors.New("wallet not found")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidOperation  = errors.New("invalid operation type")
	ErrInvalidAmount     = errors.New("invalid amount")
	ErrUserExists        = errors.New("username or email already taken")
	ErrAuthFailed        = errors.New("invalid username or password")
	ErrInvalidCurrency   = errors.New("invalid currency")
)
