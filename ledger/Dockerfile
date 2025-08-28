FROM golang:1.24 AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o ledger ./cmd/ledger

FROM alpine:latest

WORKDIR /app

COPY --from=build /app/ledger .

ENTRYPOINT ["/app/ledger"]