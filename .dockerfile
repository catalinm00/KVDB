# Etapa 1: Build
FROM golang:1.24 AS builder

# Configuramos variables de entorno para Go
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Carpeta de trabajo dentro del contenedor
WORKDIR /app

# Copiamos los archivos de go.mod y go.sum primero (para cachear dependencias)
COPY go.mod go.sum ./
RUN go mod download

# Copiamos todo el código
COPY load-testing .

# Compilamos la aplicación
RUN go build -o app ./cmd/main.go

# Etapa 2: Imagen final
FROM alpine:3.20

# Instalamos certificados para conexiones HTTPS si tu app los necesita
# RUN apk --no-cache add ca-certificates

# Carpeta de trabajo
WORKDIR /root/

# Copiamos el binario desde la etapa anterior
COPY --from=builder /app/app .

# Puerto que expone la app (ajústalo según tu proyecto)
EXPOSE 3000

# Comando por defecto
CMD ["./app"]
