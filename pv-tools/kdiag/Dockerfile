FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY pv-tools/kdiag/go.mod ./
RUN go mod download

COPY pv-tools/kdiag/ ./
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o /app/kdiag .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/kdiag .
CMD ["./kdiag"]
