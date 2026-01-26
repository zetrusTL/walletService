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
	// 1) config из env
	cfg, err := internal.LoadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// 2) подключаем pgx pool
	pool, err := connectPGXPool(cfg.PostgresDSN())
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	// 3) собираем зависимости
	repo := internal.NewPostgresRepo(pool)
	svc := internal.NewWalletService(repo)

	// 4) HTTP (пока заглушка — добавим handlers дальше)
	mux := http.NewServeMux()
	h := internal.NewHandler(svc)
	internal.RegisterRoutes(mux, h)


	// здесь позже подключим роуты кошелька:
	// internal.RegisterRoutes(mux, svc)

	server := &http.Server{
		Addr:         ":" + cfg.AppPort,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	// 5) graceful shutdown
	go func() {
		log.Printf("server started on :%s", cfg.AppPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
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

func connectPGXPool(dsn string) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	// можно настроить пул (не обязательно, но приятно)
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool new: %w", err)
	}

	// пингуем, чтобы упасть сразу, если БД недоступна
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db ping: %w", err)
	}

	return pool, nil
}
