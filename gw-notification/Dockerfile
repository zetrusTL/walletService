FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY gw-notification/ ./
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o notification ./cmd/app

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /app/notification /app/notification

CMD ["/app/notification"]
