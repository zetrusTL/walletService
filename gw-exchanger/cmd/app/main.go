package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"exchanger/internal/config"
	grpchandler "exchanger/internal/grpc"
	"exchanger/internal/service"
	"exchanger/internal/storage/postgres"
	"exchanger/internal/exchange"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Print("starting exchanger gRPC server...")
	defer func() {
		if v := recover(); v != nil {
			log.Printf("panic: %v", v)
			os.Exit(1)
		}
	}()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	pool, err := connectWithRetry(cfg.DSN(), 30*time.Second)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := postgres.Ping(ctx, pool); err != nil {
		cancel()
		log.Fatalf("db ping: %v", err)
	}
	cancel()
	log.Print("db ping OK")

	repo := postgres.NewRatesRepo(pool)
	svc := service.New(repo)
	handler := grpchandler.New(svc)

	srv := grpc.NewServer()
	exchange.RegisterExchangeServiceServer(srv, handler)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	go func() {
		log.Printf("gRPC server started on :%s", cfg.GRPCPort)
		if err := srv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	srv.GracefulStop()
	log.Println("bye")
}

func connectWithRetry(dsn string, timeout time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		cancel()
		if err != nil {
			log.Printf("db connect attempt: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
		err = pool.Ping(ctx2)
		cancel2()
		if err != nil {
			pool.Close()
			log.Printf("db ping: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		return pool, nil
	}
	return nil, context.DeadlineExceeded
}
