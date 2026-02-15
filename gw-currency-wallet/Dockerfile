# Build context must be repo root (.). Proto is at /proto for replace wallet.service/proto => ../proto
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY gw-currency-wallet/go.mod gw-currency-wallet/go.sum ./
COPY proto /proto
COPY gw-currency-wallet ./
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o wallet ./cmd/app

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /app/wallet /app/wallet

EXPOSE 8080
CMD ["/app/wallet"]
