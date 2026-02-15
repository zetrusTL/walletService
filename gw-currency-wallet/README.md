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
- **KAFKA_BROKERS** — брокеры Kafka (через запятую). Если пусто — события крупных транзакций не публикуются.
- **KAFKA_TOPIC_LARGE_TRANSACTIONS** — топик для крупных транзакций (по умолчанию `large_transactions`).
- **LARGE_TRANSACTION_THRESHOLD** — порог суммы для публикации в Kafka (по умолчанию `40000`).
- **KAFKA_PRODUCER_RETRIES** — число попыток отправки в Kafka (по умолчанию `3`).
- **KAFKA_PRODUCER_BACKOFF_MS** — базовая задержка между попытками в мс, экспоненциально растёт (по умолчанию `200`).
- **KAFKA_PRODUCER_TIMEOUT_MS** — таймаут одной попытки отправки в мс (по умолчанию `2000`).

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

#### Обмен валют — POST /api/v1/exchange

```bash
curl -X POST http://localhost:8080/api/v1/exchange \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"from_currency":"USD","to_currency":"EUR","amount":100}'
```

Ответ `200 OK`, например:
```json
{
  "message": "Exchange successful",
  "rate": 0.92,
  "exchanged_amount": 92,
  "new_balance": {"USD": 0, "EUR": 92, "RUB": 0},
  "source": "cache"
}
```
При недостатке средств по `from_currency` — `400` с `{"error":"insufficient funds"}`. Если курс пары недоступен — `400` с сообщением об ошибке.

### Крупные транзакции и Kafka

Операции (deposit/withdraw/exchange) с суммой не ниже **LARGE_TRANSACTION_THRESHOLD** после успешного выполнения публикуются в Kafka (топик `large_transactions`) для gw-notification.

- **Retry**: отправка выполняется с повторными попытками (exponential backoff). Конфиг: `KAFKA_PRODUCER_RETRIES`, `KAFKA_PRODUCER_BACKOFF_MS`, `KAFKA_PRODUCER_TIMEOUT_MS`.
- **Ответ клиенту не зависит от Kafka**: если после всех попыток публикация не удалась, в лог пишется только WARN с `transaction_id` и причиной; HTTP-ответ остаётся успешным (финансовая операция уже выполнена).
- **At-least-once + идемпотентность**: Kafka даёт at-least-once доставку (сообщение может прийти повторно). В gw-notification коллекция событий имеет уникальный индекс по `transaction_id`, поэтому повторная доставка не создаёт дубликатов — повторные события игнорируются. Безопасно увеличивать число попыток и повторно слать при сбоях.

#### Проверка: Kafka недоступен → 200 OK + WARN в логах; Kafka снова доступен → publish успешен

Убеждаемся, что при падении Kafka депозит всё равно возвращает 200, а после поднятия Kafka сообщения снова уходят.

1. Запустить стек (из корня репозитория):
   ```bash
   docker compose up -d
   ```
2. Получить токен и сделать депозит на сумму ≥ порога (по умолчанию 40000), чтобы убедиться, что всё работает:
   ```bash
   TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/login -H "Content-Type: application/json" -d '{"username":"alice","password":"secret123"}' | jq -r .token)
   curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/api/v1/wallet/deposit -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" -d '{"amount":40000,"currency":"RUB"}'
   ```
   Ожидается код `200`.

3. Остановить Kafka (имитация сбоя):
   ```bash
   docker stop kafka
   ```
4. Снова сделать депозит 40000:
   ```bash
   curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/api/v1/wallet/deposit -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" -d '{"amount":40000,"currency":"RUB"}'
   ```
   Ожидается код `200` (операция прошла, ответ клиенту успешный). В логах wallet должен появиться WARN о неудачной отправке в Kafka:
   ```bash
   docker logs wallet_app 2>&1 | tail -20
   ```
   Ищите строку вида: `WARN: kafka publish failed transaction_id=...`

5. Запустить Kafka снова:
   ```bash
   docker start kafka
   ```
   Подождать несколько секунд, пока брокер поднимется.

6. Ещё раз депозит 40000:
   ```bash
   curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/api/v1/wallet/deposit -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" -d '{"amount":40000,"currency":"RUB"}'
   ```
   Ожидается `200`. В логах wallet — успешная публикация (`kafka published large_transaction ...` или `INFO: published after retry ...`, если сработала вторая/третья попытка).

7. (Опционально) Проверить, что notification получил событие — в Mongo появилась запись:
   ```bash
   docker exec -it mongo mongosh -u mongo -p mongo --authenticationDatabase admin --eval 'db.getSiblingDB("notification").large_transactions.find().sort({created_at:-1}).limit(3).pretty()'
   ```

### Пример сценария (curl)

Получить токен, пополнить USD, обменять USD→EUR, проверить баланс:

```bash
# Токен (после регистрации и логина)
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"secret123"}' | jq -r .token)

# Депозит 100 USD
curl -s -X POST http://localhost:8080/api/v1/wallet/deposit \
  -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" \
  -d '{"amount":100,"currency":"USD"}'

# Обмен 100 USD → EUR
curl -s -X POST http://localhost:8080/api/v1/exchange \
  -H "Content-Type: application/json" -H "Authorization: Bearer $TOKEN" \
  -d '{"from_currency":"USD","to_currency":"EUR","amount":100}'

# Баланс (должны быть USD=0, EUR=92 при курсе 0.92)
curl -s http://localhost:8080/api/v1/balance -H "Authorization: Bearer $TOKEN"
```

Проверка недостатка средств: повторить обмен без повторного депозита — ожидается `400` и `"error":"insufficient funds"`.

### Health endpoint — GET /health

Проверяет доступность зависимостей: DB, exchanger (gRPC), Kafka.

```bash
curl -s http://localhost:8080/health
```

Пример ответа при успехе:
```json
{"status":"ok","db":"ok","exchanger":"ok","kafka":"ok"}
```

При недоступности любой зависимости — `status: "degraded"`, HTTP 503, поля с ошибкой: `"error"`.

### Ошибки

    400	invalid amount / invalid currency / invalid json / Insufficient funds / insufficient funds (exchange) / from_currency and to_currency must differ / exchange rate not available
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
