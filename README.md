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

Генерация выполняется локально; в Docker пока не настроена.
