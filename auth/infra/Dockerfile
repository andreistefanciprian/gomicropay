FROM golang:1.24 AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN ls -larth

RUN CGO_ENABLED=0 GOOS=linux go build -o auth ./cmd/auth

# Final stage - using distroless
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=build /app/auth .

# Use the nonroot user (UID 65532)
USER nonroot:nonroot

ENTRYPOINT ["/app/auth"]