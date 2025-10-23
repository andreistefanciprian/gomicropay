FROM golang:1.25@sha256:8c945d3e25320e771326dafc6fb72ecae5f87b0f29328cbbd87c4dff506c9135 AS build

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