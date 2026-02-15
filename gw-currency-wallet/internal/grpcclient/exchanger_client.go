package grpcclient

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	exchange "wallet.service/proto/exchange"
)

// ExchangerClient wraps gRPC connection to exchanger and provides cached rates.
type ExchangerClient struct {
	conn     *grpc.ClientConn
	client   exchange.ExchangeServiceClient
	timeout  time.Duration
	cacheTTL time.Duration

	mu         sync.RWMutex
	cacheRates map[string]float64
	cacheAt    time.Time
}

// NewExchangerClient dials the exchanger and returns a client. addr may be empty (then all calls return empty/zero).
func NewExchangerClient(addr string, timeout, cacheTTL time.Duration) (*ExchangerClient, error) {
	if addr == "" {
		return &ExchangerClient{
			timeout:  timeout,
			cacheTTL: cacheTTL,
			cacheRates: map[string]float64{},
		}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	return &ExchangerClient{
		conn:       conn,
		client:     exchange.NewExchangeServiceClient(conn),
		timeout:    timeout,
		cacheTTL:   cacheTTL,
		cacheRates: map[string]float64{},
	}, nil
}

// Close closes the gRPC connection.
func (c *ExchangerClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// GetAllRates returns all rates, using cache if valid; otherwise fetches via gRPC. source is "cache" or "grpc".
func (c *ExchangerClient) GetAllRates(ctx context.Context) (rates map[string]float64, source string, err error) {
	c.mu.RLock()
	if !c.cacheAt.IsZero() && time.Since(c.cacheAt) < c.cacheTTL {
		// Copy map so caller cannot mutate cache
		out := make(map[string]float64, len(c.cacheRates))
		for k, v := range c.cacheRates {
			out[k] = v
		}
		c.mu.RUnlock()
		log.Printf("INFO: rates cache hit")
		return out, "cache", nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	// Double-check after acquiring write lock
	if !c.cacheAt.IsZero() && time.Since(c.cacheAt) < c.cacheTTL {
		out := make(map[string]float64, len(c.cacheRates))
		for k, v := range c.cacheRates {
			out[k] = v
		}
		log.Printf("INFO: rates cache hit")
		return out, "cache", nil
	}

	if c.client == nil {
		return map[string]float64{}, "grpc", nil
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.client.GetExchangeRates(callCtx, &exchange.Empty{})
	if err != nil {
		return nil, "", err
	}
	log.Printf("INFO: fetched rates from exchanger")
	c.cacheRates = resp.GetRates()
	if c.cacheRates == nil {
		c.cacheRates = map[string]float64{}
	}
	c.cacheAt = time.Now()
	out := make(map[string]float64, len(c.cacheRates))
	for k, v := range c.cacheRates {
		out[k] = v
	}
	return out, "grpc", nil
}

// GetRate returns rate for from->to; uses cache if valid, else one gRPC call (and updates cache for all rates via GetAllRates).
func (c *ExchangerClient) GetRate(ctx context.Context, from, to string) (float64, error) {
	key := from + "_" + to
	rates, _, err := c.GetAllRates(ctx)
	if err != nil {
		return 0, err
	}
	if v, ok := rates[key]; ok {
		return v, nil
	}
	if c.client == nil {
		return 0, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.client.GetExchangeRateForCurrency(callCtx, &exchange.CurrencyRequest{
		FromCurrency: from,
		ToCurrency:   to,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetRate(), nil
}
