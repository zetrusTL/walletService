FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY gw-exchanger/ ./
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o exchanger ./cmd/app

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /app/exchanger /app/exchanger

EXPOSE 9090
CMD ["/app/exchanger"]
