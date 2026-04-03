FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /sidecar ./cmd/sidecar

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /sidecar /usr/local/bin/sidecar

EXPOSE 9500 50051

ENTRYPOINT ["sidecar"]
