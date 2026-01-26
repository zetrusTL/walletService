package main_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestConcurrentDeposits(t *testing.T) {
	const baseURL = "http://localhost:8080"
	const walletID = "11111111-1111-1111-1111-111111111111"

	const N = 200

	type opReq struct {
		WalletID      string `json:"walletId"`
		OperationType string `json:"operationType"`
		Amount        int64  `json:"amount"`
	}

	client := &http.Client{Timeout: 5 * time.Second}

	wg := sync.WaitGroup{}
	wg.Add(N)

	errCh := make(chan error, N)

	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()

			body, _ := json.Marshal(opReq{
				WalletID:      walletID,
				OperationType: "DEPOSIT",
				Amount:        1,
			})

			resp, err := client.Post(baseURL+"/api/v1/wallet", "application/json", bytes.NewReader(body))
			if err != nil {
				errCh <- err
				return
			}
			_ = resp.Body.Close()

			if resp.StatusCode != 200 {
				errCh <-  &httpError{code: resp.StatusCode}
				return
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("request failed: %v", err)
	}

	resp, err := client.Get(baseURL + "/api/v1/wallets/" + walletID)
	if err != nil {
		t.Fatalf("get balance: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("get balance status: %d", resp.StatusCode)
	}

	var out struct {
		WalletID string `json:"walletId"`
		Balance  int64  `json:"balance"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out.Balance < N {
		t.Fatalf("expected balance >= %d, got %d", N, out.Balance)
	}
}

type httpError struct{ code int }
func (e *httpError) Error() string { return "bad http status" }
