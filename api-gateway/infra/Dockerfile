FROM golang:1.25@sha256:0afe9b553123ec160b8282154b7156241ebf43c9033ea2bf9528c6c58c17e89d AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot@sha256:e8a4044e0b4ae4257efa45fc026c0bc30ad320d43bd4c1a7d5271bd241e386d0

WORKDIR /app

COPY --from=build /app/gateway .

# Use the nonroot user (UID 65532)
USER nonroot:nonroot

EXPOSE 8080

ENTRYPOINT ["/app/gateway"]