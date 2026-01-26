
FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o wallet ./cmd/app


FROM alpine:3.20

WORKDIR /app
COPY --from=builder /app/wallet /app/wallet

EXPOSE 8080
CMD ["/app/wallet"]
