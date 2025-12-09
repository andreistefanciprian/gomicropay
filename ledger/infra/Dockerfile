FROM golang:1.25@sha256:0ece421d4bb2525b7c0b4cad5791d52be38edf4807582407525ca353a429eccc AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o ledger ./cmd/ledger

FROM gcr.io/distroless/static-debian12:nonroot@sha256:2b7c93f6d6648c11f0e80a48558c8f77885eb0445213b8e69a6a0d7c89fc6ae4

WORKDIR /app

COPY --from=build /app/ledger .

# Use the nonroot user (UID 65532)
USER nonroot:nonroot

ENTRYPOINT ["/app/ledger"]