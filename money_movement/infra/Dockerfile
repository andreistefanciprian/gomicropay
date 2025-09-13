FROM golang:1.24 AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o money_movement ./cmd/money_movement

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=build /app/money_movement .

# Use the nonroot user (UID 65532)
USER nonroot:nonroot

ENTRYPOINT ["/app/money_movement"]