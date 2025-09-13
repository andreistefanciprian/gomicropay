FROM golang:1.24 AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o email ./cmd/email

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=build /app/email .

# Use the nonroot user (UID 65532)
USER nonroot:nonroot

ENTRYPOINT ["/app/email"]