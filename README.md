# walletService (монорепо)

Монорепо для сервисов: gw-currency-wallet, gw-exchanger, gw-notification.

## How to run

Из корня репозитория:

```bash
docker-compose up --build
```

Сервисы: wallet — `http://localhost:8080`, exchanger gRPC — `:9090`, notification health — `http://localhost:8081`, Postgres — `:5433`, Mongo — `:27017`.

### Register / Login

```bash
# Регистрация
curl -X POST http://localhost:8080/api/v1/register \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","email":"alice@example.com","password":"secret123"}'

# Логин (сохранить токен)
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"secret123"}' | jq -r .token)
```

### Balance / Deposit / Withdraw

```bash
# Баланс
curl -s http://localhost:8080/api/v1/balance -H "Authorization: Bearer $TOKEN"

# Депозит
curl -X POST http://localhost:8080/api/v1/wallet/deposit \
  -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" \
  -d '{"amount":100,"currency":"USD"}'

# Снятие
curl -X POST http://localhost:8080/api/v1/wallet/withdraw \
  -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" \
  -d '{"amount":50,"currency":"USD"}'
```

### Exchange

```bash
curl -s -X POST http://localhost:8080/api/v1/exchange \
  -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" \
  -d '{"from_currency":"USD","to_currency":"EUR","amount":100}'
```

В ответе поле `source`: **`grpc`** — курс взят с exchanger по gRPC, **`cache`** — из локального кеша (TTL 10s по умолчанию).

Подробнее: [gw-currency-wallet/README.md](gw-currency-wallet/README.md).

---

## How to test

### 1. Large transaction → Mongo

Депозит ≥40000 (LARGE_TRANSACTION_THRESHOLD) публикует событие в Kafka; notification сохраняет в Mongo.

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/login -H "Content-Type: application/json" -d '{"username":"alice","password":"secret123"}' | jq -r .token)
curl -X POST http://localhost:8080/api/v1/wallet/deposit \
  -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" \
  -d '{"amount":40000,"currency":"RUB"}'

# Проверка в Mongo
docker exec -it mongo mongosh -u mongo -p mongo --authenticationDatabase admin --eval \
  'db.getSiblingDB("notification").large_transactions.find().sort({created_at:-1}).limit(3).pretty()'
```

### 2. Idempotency

Повторно отправленное событие (duplicate) не создаёт дубликат: коллекция имеет уникальный индекс по `transaction_id`. При at-least-once доставке Kafka повторные сообщения игнорируются (E11000 duplicate key).

### 3. Retry (Kafka unavailable)

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/login -H "Content-Type: application/json" -d '{"username":"alice","password":"secret123"}' | jq -r .token)

# Остановить Kafka
docker stop kafka

# Депозит 40000 — ожидается 200 OK, в логах WARN
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/api/v1/wallet/deposit \
  -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" -d '{"amount":40000,"currency":"RUB"}'
# → 200
docker logs wallet_app 2>&1 | tail -10
# → WARN: kafka publish failed transaction_id=...

# Запустить Kafka
docker start kafka
# Подождать 5–10 сек

# Повторный депозит 40000 — сообщение опубликовано
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/api/v1/wallet/deposit \
  -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" -d '{"amount":40000,"currency":"RUB"}'
# → 200
docker logs wallet_app 2>&1 | tail -5
# → kafka published large_transaction ...
```

---

## Architecture

- **wallet** → exchanger (gRPC) → Postgres — получение курсов валют и учёт баланса
- **wallet** → Kafka → **notification** → Mongo — события крупных транзакций (≥ порога)

Kafka даёт **at-least-once** доставку; notification обеспечивает **идемпотентность** через уникальный индекс по `transaction_id` — дубликаты не создаются.

**Health endpoints**: `GET /health` — wallet (8080), notification (8081). Проверяют DB, exchanger, Kafka / Mongo, Kafka.

---

## Health check

| Сервис | URL | Проверяет |
|--------|-----|-----------|
| wallet | `curl http://localhost:8080/health` | DB, exchanger gRPC, Kafka |
| notification | `curl http://localhost:8081/health` | Mongo, Kafka |

При успехе: `{"status":"ok", ...}`. Docker Compose healthcheck использует эти endpoint'ы для определения готовности контейнеров.

## Генерация gRPC-кода (proto/exchange)

Установка плагинов (один раз):

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Генерация из корня репозитория:

```bash
protoc --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative proto/exchange/exchange.proto
```

Генерация выполняется локально; в Docker пока не настроена. После изменения proto скопируйте сгенерированные файлы в gw-exchanger:

```bash
cp proto/exchange/exchange.pb.go proto/exchange/exchange_grpc.pb.go gw-exchanger/internal/exchange/
```

## Запуск exchanger (gRPC)

Вместе с wallet поднимаются сервисы **exchanger** (gRPC на порту **9090**) и миграции для БД `exchanger`. Проверка:

- В логах контейнера `exchanger_app`: `gRPC server started on :9090`, `db ping OK`.
- Тест клиентом из репозитория (при запущенном exchanger):

```bash
cd gw-exchanger && go run ./cmd/client -addr localhost:9090
```

- Через grpcurl (если установлен):

```bash
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext localhost:9090 exchange.ExchangeService/GetExchangeRates
grpcurl -plaintext -d '{"from_currency":"USD","to_currency":"RUB"}' localhost:9090 exchange.ExchangeService/GetExchangeRateForCurrency
```
