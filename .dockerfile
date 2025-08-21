# Etapa 1: Build
FROM golang:1.24 AS builder

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o app ./main.go

# Etapa 2: Imagen final
FROM alpine:3.20

WORKDIR /root/
COPY --from=builder /app/app .

EXPOSE 3000
ENTRYPOINT ["./app"]
