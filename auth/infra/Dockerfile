FROM golang:1.25@sha256:e68f6a00e88586577fafa4d9cefad1349c2be70d21244321321c407474ff9bf2 AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN ls -larth

RUN CGO_ENABLED=0 GOOS=linux go build -o auth ./cmd/auth

# Final stage - using distroless
FROM gcr.io/distroless/static-debian12:nonroot@sha256:e8a4044e0b4ae4257efa45fc026c0bc30ad320d43bd4c1a7d5271bd241e386d0

WORKDIR /app

COPY --from=build /app/auth .

# Use the nonroot user (UID 65532)
USER nonroot:nonroot

ENTRYPOINT ["/app/auth"]