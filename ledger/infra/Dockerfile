FROM golang:1.25@sha256:20b91eda7a9627c127c0225b0d4e8ec927b476fa4130c6760928b849d769c149 AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o ledger ./cmd/ledger

FROM gcr.io/distroless/static-debian12:nonroot@sha256:e8a4044e0b4ae4257efa45fc026c0bc30ad320d43bd4c1a7d5271bd241e386d0

WORKDIR /app

COPY --from=build /app/ledger .

# Use the nonroot user (UID 65532)
USER nonroot:nonroot

ENTRYPOINT ["/app/ledger"]