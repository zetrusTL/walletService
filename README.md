# walletService (монорепо)

Монорепо для сервисов: gw-currency-wallet, gw-exchanger, gw-notification, pv-producer, pv-aggregator.

## How to run

Из корня репозитория:

```bash
docker-compose up --build
```

Сервисы: wallet — `http://localhost:8080`, exchanger gRPC — `:9090`, notification health — `http://localhost:8081`, pv-producer — `http://localhost:8082`, pv-aggregator — `http://localhost:8083`, Postgres — `:5433`, Mongo — `:27017`, ClickHouse — `:8123` (HTTP), `:9000` (native).

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
| pv-producer | `curl http://localhost:8082/metrics` | Metrics endpoint |
| pv-aggregator | `curl http://localhost:8083/health` | Health check |

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

---

## Page Views Aggregation System

Система real-time агрегации page view событий: **pv-producer** (генератор событий) → Kafka → **pv-aggregator** (consumer) → ClickHouse.

### Архитектура

- **pv-producer**: Генерирует события page views и отправляет в Kafka topic `page_views`
- **pv-aggregator**: Читает из Kafka, валидирует, агрегирует в батчи и пишет в ClickHouse
- **ClickHouse**: Хранит сырые события (`page_views_raw`), автоматически агрегирует в минуты (`page_views_agg_minute`) и часы (`page_views_agg_hour`) через материализованные представления
- **DLQ**: Невалидные события отправляются в `page_views_dlq` и записываются в `processing_errors`

### Запуск и проверка

```bash
# Запустить все сервисы
docker-compose up --build

# Дождаться готовности (проверить логи)
docker logs pv_producer
docker logs pv_aggregator
docker logs clickhouse_init  # Должен завершиться успешно
```

### Управление генерацией событий

```bash
# Режим regular (1-10 событий/сек)
curl -X POST http://localhost:8082/control \
  -H "Content-Type: application/json" \
  -d '{"mode":"regular","rps":5}'

# Режим burst (100-1000 событий за 2 секунды)
curl -X POST http://localhost:8082/control \
  -H "Content-Type: application/json" \
  -d '{"mode":"burst"}'

# Режим night (1 событие в 10 секунд)
curl -X POST http://localhost:8082/control \
  -H "Content-Type: application/json" \
  -d '{"mode":"night"}'

# Изменить параметры batch отправки
curl -X POST http://localhost:8082/control \
  -H "Content-Type: application/json" \
  -d '{"send_mode":"batch","batch_size":500,"flush_ms":2000,"strategy":"key"}'
```

### Проверка данных в ClickHouse

```bash
# Подключиться к ClickHouse
docker exec -it clickhouse clickhouse-client

# Проверить сырые события
SELECT count() FROM page_views_raw;
SELECT * FROM page_views_raw ORDER BY event_time DESC LIMIT 10;

# Проверить минутные агрегации
SELECT 
    window_start,
    page_id,
    sumMerge(view_count) as views,
    sumMerge(total_duration) / sumMerge(view_count) as avg_duration_ms,
    uniqMerge(unique_users) as unique_users,
    sumMerge(bounce_count) * 100.0 / sumMerge(view_count) as bounce_rate_pct
FROM page_views_agg_minute
GROUP BY window_start, page_id
ORDER BY window_start DESC
LIMIT 20;

# Проверить часовые агрегации (с avg_duration и bounce_rate)
SELECT * FROM page_views_agg_hour_read ORDER BY window_start DESC LIMIT 10;

# Проверить ошибки (DLQ)
SELECT count() FROM processing_errors;
SELECT * FROM processing_errors ORDER BY error_time DESC LIMIT 10;
```

### Метрики

```bash
# Метрики producer
curl http://localhost:8082/metrics | jq

# Метрики aggregator
curl http://localhost:8083/metrics | jq
```

### Тестирование DLQ и обработки ошибок

Producer генерирует 5% ошибок:
- Пустой `page_id`
- Отрицательная `duration_ms`
- Некорректный JSON

Эти события должны попасть в DLQ и в таблицу `processing_errors`:

```bash
# Проверить ошибки
docker exec -it clickhouse clickhouse-client -q "SELECT count(), error_reason FROM processing_errors GROUP BY error_reason"
```

### Тестирование at-least-once гарантий

```bash
# Остановить ClickHouse
docker stop clickhouse

# Producer продолжит генерировать события
curl -X POST http://localhost:8082/control -H "Content-Type: application/json" -d '{"mode":"regular","rps":10}'

# Aggregator будет ретраить вставки и НЕ будет коммитить offsets
# Проверить логи:
docker logs pv_aggregator | grep -i "retry\|commit"

# Запустить ClickHouse обратно
docker start clickhouse

# Подождать несколько секунд - aggregator должен догрузить все события
# Проверить что данные появились:
docker exec -it clickhouse clickhouse-client -q "SELECT count() FROM page_views_raw"
```

### Проверка дубликатов

Producer генерирует 1% дубликатов (одинаковый `event_id` и `page_id`). При at-least-once доставке Kafka могут быть повторные сообщения. ClickHouse таблица `page_views_raw` не имеет уникального индекса, поэтому дубликаты будут записаны (это ожидаемо для raw таблицы). Для дедупликации можно использовать `ReplacingMergeTree` или дедуплицировать на уровне агрегаций.

### Режимы работы producer

- **sync**: Синхронная отправка, ожидание подтверждения
- **async**: Асинхронная отправка с callback
- **batch**: Батчинг (накапливает N сообщений или ждёт X мс)

### Стратегии партиционирования

- **key**: По `page_id` (гарантирует порядок для одной страницы)
- **rr**: Round-robin (равномерное распределение)
- **random**: Случайное распределение

### Consumer режимы (pv-aggregator)

Aggregator использует **hybrid** режим:
- Батчинг по размеру (`BATCH_SIZE=1000`)
- Батчинг по времени (`FLUSH_INTERVAL_MS=2000`)
- Flush происходит при достижении любого условия

Это обеспечивает баланс между latency и throughput.
