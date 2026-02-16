package model

import (
	"encoding/json"
	"time"
)

type PageViewEvent struct {
	EventID       string    `json:"event_id"`
	PageID        string    `json:"page_id"`
	UserID        string    `json:"user_id"`
	ViewDuration  int       `json:"view_duration_ms"`
	Timestamp     time.Time `json:"timestamp"`
	UserAgent     string    `json:"user_agent,omitempty"`
	IPAddress     string    `json:"ip_address,omitempty"`
	Region        string    `json:"region,omitempty"`
	IsBounce      bool      `json:"is_bounce"`
}

func (e *PageViewEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}
