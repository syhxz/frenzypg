FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=$(git describe --tags --always 2>/dev/null || echo docker) -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o /frenzy ./cmd

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata
RUN adduser -D -s /bin/sh frenzy

COPY --from=builder /frenzy /usr/local/bin/frenzy

USER frenzy
EXPOSE 5432 9090

ENTRYPOINT ["/usr/local/bin/frenzy"]
CMD ["--listen", ":5432", "--health-port", "9090"]
