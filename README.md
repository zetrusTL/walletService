# walletService (монорепо)

Монорепо для сервисов: gw-currency-wallet, gw-exchanger, gw-notification.

## Запуск wallet и Postgres

Из корня репозитория:

```bash
docker compose up --build
```

Сервис wallet будет доступен на `http://localhost:8080`, Postgres — на порту `5433`.

Подробнее см. [gw-currency-wallet/README.md](gw-currency-wallet/README.md).

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
