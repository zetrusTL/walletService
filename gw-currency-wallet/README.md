# gw-currency-wallet

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

### Переменные окружения

- **JWT_SECRET** — секрет для подписи JWT (обязательно).
- **DB_HOST**, **DB_PORT**, **DB_USER**, **DB_PASSWORD**, **DB_NAME** — подключение к PostgreSQL.
- **DB_SSLMODE** — опционально (по умолчанию `disable`).
- **APP_PORT** — порт приложения (по умолчанию `8080`).

### Запуск через Docker (из корня монорепо)

```bash
# Из корня репозитория (миграции применяются автоматически сервисом migrator):
docker compose up --build
```

Проверка, что таблицы созданы:
```bash
docker compose exec db psql -U wallet -d wallet -c '\dt'
```
Ожидаются таблицы: `balances`, `users`, `wallet_legacy`, `wallets`.

### Регистрация и логин (JWT)

#### Регистрация — POST /api/v1/register

```bash
curl -X POST http://localhost:8080/api/v1/register \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","email":"alice@example.com","password":"secret123"}'
```

Ответ: `201 Created` (при успехе). При занятом username/email — `409 Conflict`.

#### Логин — POST /api/v1/login

```bash
curl -X POST http://localhost:8080/api/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"secret123"}'
```

Ответ: `200 OK` и JSON с полем `token` (JWT). Далее передавайте его в заголовке: `Authorization: Bearer <token>`.

### Баланс и операции (требуется JWT)

Во всех запросах ниже подставьте свой токен: `TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/login -H "Content-Type: application/json" -d '{"username":"alice","password":"secret123"}' | jq -r .token)` и используйте `-H "Authorization: Bearer $TOKEN"`.

#### Получить баланс — GET /api/v1/balance

```bash
curl -s http://localhost:8080/api/v1/balance -H "Authorization: Bearer $TOKEN"
```

Ответ: `200 OK`, например:
```json
{"balance":{"USD":0,"RUB":0,"EUR":0}}
```

#### Пополнение — POST /api/v1/wallet/deposit

```bash
curl -X POST http://localhost:8080/api/v1/wallet/deposit \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"amount":100,"currency":"USD"}'
```

Ответ: `200 OK`, например: `{"currency":"USD","amount":100}`.

#### Снятие — POST /api/v1/wallet/withdraw

```bash
curl -X POST http://localhost:8080/api/v1/wallet/withdraw \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"amount":50,"currency":"USD"}'
```

Ответ: `200 OK`, например: `{"currency":"USD","amount":50}`. При недостатке средств — `400` с телом `{"error":"Insufficient funds"}`.

Поддерживаемые валюты: `USD`, `RUB`, `EUR`.

### Ошибки

    400	invalid amount / invalid currency / invalid json / Insufficient funds
    401	missing or invalid Authorization (Bearer token)
    404	wallet not found
    500	internal error

### Тесты:

    интеграционный: 
    docker compose up -d   # из корня репозитория
    cd gw-currency-wallet && go test -v ./concurrency_test.go

    юнит:
    cd gw-currency-wallet && go test ./internal -v



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
