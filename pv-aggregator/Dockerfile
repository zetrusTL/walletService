FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY pv-aggregator/go.mod pv-aggregator/go.sum* ./
RUN go mod download

COPY pv-aggregator/ ./
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o /app/pv-aggregator ./cmd/app

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/pv-aggregator .
CMD ["./pv-aggregator"]
