# walletService (монорепо)

Монорепо для сервисов: gw-currency-wallet, gw-exchanger, gw-notification.

## Запуск wallet и Postgres

Из корня репозитория:

```bash
docker compose up --build
```

Сервис wallet будет доступен на `http://localhost:8080`, Postgres — на порту `5433`.

Подробнее см. [gw-currency-wallet/README.md](gw-currency-wallet/README.md).
