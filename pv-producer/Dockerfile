FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY pv-producer/go.mod pv-producer/go.sum* ./
RUN go mod download

COPY pv-producer/ ./
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o /app/pv-producer ./cmd/app

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/pv-producer .
CMD ["./pv-producer"]
