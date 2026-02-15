package grpcclient

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	exchange "wallet.service/proto/exchange"
)

type ExchangerClient struct { // клиент для работы с exchanger
	conn     *grpc.ClientConn
	client   exchange.ExchangeServiceClient
	timeout  time.Duration
	cacheTTL time.Duration

	mu         sync.RWMutex
	cacheRates map[string]float64
	cacheAt    time.Time
}

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

func (c *ExchangerClient) Close() error { // закрытие соединения
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *ExchangerClient) GetAllRates(ctx context.Context) (rates map[string]float64, source string, err error) {
	c.mu.RLock()
	if !c.cacheAt.IsZero() && time.Since(c.cacheAt) < c.cacheTTL {
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

func (c *ExchangerClient) GetRateWithSource(ctx context.Context, from, to string) (rate float64, source string, err error) {
	key := from + "_" + to
	rates, src, err := c.GetAllRates(ctx)
	if err != nil {
		return 0, "", err
	}
	if v, ok := rates[key]; ok {
		return v, src, nil
	}
	if c.client == nil {
		return 0, "", fmt.Errorf("exchange rate %s not found", key)
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	resp, err := c.client.GetExchangeRateForCurrency(callCtx, &exchange.CurrencyRequest{
		FromCurrency: from,
		ToCurrency:   to,
	})
	if err != nil {
		return 0, "", err
	}
	r := resp.GetRate()
	if r <= 0 {
		return 0, "", fmt.Errorf("exchange rate %s not found", key)
	}
	return r, "grpc", nil
}
