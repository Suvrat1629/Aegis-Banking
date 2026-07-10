## Build stage
FROM golang:1.26-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Copy go mod files first for dependency caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source code
COPY . ./

# Build the binary from cmd/server
WORKDIR /src/cmd/server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/notification-server .

## Run stage
FROM alpine:3.18

# Install runtime dependencies
RUN apk add --no-cache ca-certificates

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/notification-server /usr/local/bin/notification-server

# Expose metrics port
EXPOSE 2114

ENTRYPOINT ["/usr/local/bin/notification-server"]
