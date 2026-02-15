package model

// LargeTransactionEvent — формат сообщения из Kafka (как публикует wallet).
type LargeTransactionEvent struct {
	TransactionID string  `json:"transaction_id"`
	UserID        int     `json:"user_id"`
	Type          string  `json:"type"` // "deposit"|"withdraw"|"exchange"
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	CreatedAt     string  `json:"created_at"` // RFC3339
}
