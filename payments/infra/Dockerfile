FROM golang:1.25@sha256:bc45dfd319e982dffe4de14428c77defe5b938e29d9bc6edfbc0b9a1fc171cb3 AS build

WORKDIR /app

COPY go.mod ./

RUN go mod tidy

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o payments ./cmd/payments

FROM gcr.io/distroless/static-debian12:nonroot@sha256:2b7c93f6d6648c11f0e80a48558c8f77885eb0445213b8e69a6a0d7c89fc6ae4

WORKDIR /app

COPY --from=build /app/payments .

# Use the nonroot user (UID 65532)
USER nonroot:nonroot

ENTRYPOINT ["/app/payments"]