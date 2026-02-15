package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wall/internal"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	log.Print("starting wallet app...")
	defer func() {
		if v := recover(); v != nil {
			log.Printf("panic: %v", v)
			os.Exit(1)
		}
	}()

	cfg, err := internal.LoadConfig()
	if err != nil {
		log.Printf("load config: %v", err)
		os.Exit(1)
	}

	pool, err := connectPGXPoolWithRetry(cfg.PostgresDSN(), 30*time.Second)
	if err != nil {
		log.Printf("connect db: %v", err)
		os.Exit(1)
	}
	defer pool.Close()
	repo := internal.NewPostgresRepo(pool) 
	svc := internal.NewWalletService(repo)
	authRepo := internal.NewAuthRepo(pool)

	mux := http.NewServeMux()
	h := internal.NewHandler(svc, authRepo, []byte(cfg.JWTSecret))
	internal.RegisterRoutes(mux, h, []byte(cfg.JWTSecret))

	server := &http.Server{
		Addr:         ":" + cfg.AppPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	go func() {
		defer func() {
			if v := recover(); v != nil {
				log.Printf("panic in server: %v", v)
				os.Exit(1)
			}
		}()
		log.Printf("server started on :%s", cfg.AppPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("listen: %v", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Println("shutting down...")
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
	log.Println("bye")
}

func connectPGXPoolWithRetry(dsn string, timeout time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
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
	return nil, fmt.Errorf("db not ready after %v", timeout)
}

func connectPGXPool(dsn string) (*pgxpool.Pool, error) {
	return connectPGXPoolWithRetry(dsn, 5*time.Second)
}
