# Base Image
FROM golang:1.24-alpine AS builder

#Working Directory
WORKDIR /app

# Setting up the dist files
COPY go.mod go.sum ./
RUN go mod download

# Copying the rest of things
COPY . .

# Building the service
RUN go build -o distributed-daftar  ./cmd/distributed-daftar

# Second stage for running the compiled binary
FROM alpine:latest
WORKDIR  /app
COPY --from=builder /app/distributed-daftar .
EXPOSE 8080
CMD ["./distributed-daftar"]