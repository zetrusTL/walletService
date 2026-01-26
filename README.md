# Wallet Service

HTTP-сервис для управления балансом кошельков.

Поддерживает:

- получение баланса
- пополнение (DEPOSIT)
- списание (WITHDRAW)
- конкурентную работу с одним кошельком (атомарные операции в БД)

Реализован на Go, PostgreSQL.

---

## 🚀 Запуск

### Требования

- Docker
- docker-compose
- Go (для запуска тестов локально)

---

### Запуск через Docker

Из корня проекта:

```bash
docker-compose up --build
```

### Получить баланс:
    GET /api/v1/wallets/{walletId}
```bash
    (curl localhost:8080/api/v1/wallets/11111111-1111-1111-1111-111111111111)
```
    Ожидание: ({
                "walletId": "11111111-1111-1111-1111-111111111111",
                "balance": 1000
                    } )

### Операция с кошельком
    POST /api/v1/wallet

#### Пример DEPOSIT
```bash
curl -X POST localhost:8080/api/v1/wallet \
-H "Content-Type: application/json" \
-d '{"walletId":"11111111-1111-1111-1111-111111111111","operationType":"DEPOSIT","amount":100}'
```        
#### Пример WITHDRAW
```bash
curl -X POST localhost:8080/api/v1/wallet \
-H "Content-Type: application/json" \
-d '{"walletId":"11111111-1111-1111-1111-111111111111","operationType":"WITHDRAW","amount":50}'
```



### Ошибки:

    400	invalid amount / invalid operation / invalid json
    404	wallet not found
    409	insufficient funds
    500	internal error

### Тесты:

    интеграционный: 
    docker-compose up -d
    go test -v ./concurrency_test.go

    юнит:
    go test ./internal -v



### Архитектура

    Проект разделён на слои:
    handler — HTTP
    service — бизнес-логика
    repository — PostgreSQL
    models — доменные структуры

### Используемые технологии
    Go
    PostgreSQL
    pgx
    Docker / docker-compose
    UUID
    net/http