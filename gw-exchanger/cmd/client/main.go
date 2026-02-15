// Small gRPC client to test exchanger. Run after exchanger is up:
//   go run ./cmd/client -addr localhost:9090
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"exchanger/internal/exchange"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:9090", "exchanger gRPC address")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := exchange.NewExchangeServiceClient(conn)

	// GetExchangeRates
	rates, err := client.GetExchangeRates(ctx, &exchange.Empty{})
	if err != nil {
		log.Fatalf("GetExchangeRates: %v", err)
	}
	log.Printf("GetExchangeRates: %v", rates.GetRates())

	// GetExchangeRateForCurrency
	resp, err := client.GetExchangeRateForCurrency(ctx, &exchange.CurrencyRequest{
		FromCurrency: "USD",
		ToCurrency:    "RUB",
	})
	if err != nil {
		log.Fatalf("GetExchangeRateForCurrency: %v", err)
	}
	log.Printf("USD -> RUB: %.4f", resp.GetRate())

	log.Println("OK")
}
