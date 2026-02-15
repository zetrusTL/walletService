# gw-notification

Kafka consumer, который читает события крупных транзакций из топика `large_transactions` и сохраняет их в MongoDB.

## Идемпотентность

Kafka гарантирует **at-least-once** доставку: одно и то же сообщение может быть доставлено несколько раз (рестарт консьюмера, повторная обработка после сбоя). Чтобы не дублировать записи в MongoDB:

- При старте сервиса в коллекции `large_transactions` создаётся **уникальный индекс по полю `transaction_id`**.
- При вставке документа с уже существующим `transaction_id` Mongo возвращает ошибку duplicate key (E11000). Консьюмер трактует её как «событие уже обработано»: логирует `duplicate event ignored` и `duplicate transaction ignored: <transaction_id>`, коммитит offset и не повторяет вставку. Остальные ошибки приводят к retry без коммита.

## Конфигурация (env)

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| KAFKA_BROKERS | kafka:9092 | Брокеры Kafka (через запятую) |
| KAFKA_TOPIC | large_transactions | Топик |
| KAFKA_GROUP_ID | notification-service | Consumer group |
| MONGO_URI | — | URI MongoDB (обязательно) |
| MONGO_DB | notification | База данных |
| MONGO_COLLECTION | large_transactions | Коллекция |

## Формат сообщения (JSON)

Wallet публикует `LargeTransactionEvent`:

- `transaction_id` string
- `user_id` int
- `type` string ("deposit"|"withdraw"|"exchange")
- `amount` number
- `currency` string
- `created_at` string (RFC3339)

## Запуск

```bash
docker-compose up -d notification
```

## Проверка

1. Сделать в wallet депозит 40 000 RUB (или другую крупную сумму, которую wallet публикует в Kafka).
2. Посмотреть документы в Mongo:

```bash
docker exec -it mongo mongosh -u mongo -p mongo --authenticationDatabase admin --eval '
  db = db.getSiblingDB("notification");
  db.large_transactions.find().pretty();
'
```

Либо подключиться к Mongo по `localhost:27017` (user: mongo, pass: mongo, authSource: admin) и выполнить в `notification.large_transactions`:

```js
db.large_transactions.find().pretty()
```
