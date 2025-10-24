FROM golang:1.25@sha256:dd08f769578a5f51a22bf6a81109288e23cfe2211f051a5c29bd1c05ad3db52a AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o email ./cmd/email

FROM gcr.io/distroless/static-debian12:nonroot@sha256:e8a4044e0b4ae4257efa45fc026c0bc30ad320d43bd4c1a7d5271bd241e386d0

WORKDIR /app

COPY --from=build /app/email .

# Use the nonroot user (UID 65532)
USER nonroot:nonroot

ENTRYPOINT ["/app/email"]