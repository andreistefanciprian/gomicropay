package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	authpb "github.com/andreistefanciprian/gomicropay/api_gateway/auth/proto"
	"github.com/andreistefanciprian/gomicropay/api_gateway/internal/tracing"
	paymentspb "github.com/andreistefanciprian/gomicropay/api_gateway/payments/proto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	// Initialize structured logger
	logger := initLogger()

	// Initialize tracer
	tp, err := tracing.InitTracer("api-gateway")
	if err != nil {
		logger.Fatalf("failed to initialize tracer: %v", err)
	}
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("api-gateway")

	// Service addresses
	authHost := os.Getenv("AUTH_HOST")
	authPort := os.Getenv("AUTH_PORT")
	apiGatewayPort := os.Getenv("API_GATEWAY_PORT")
	paymentsHost := os.Getenv("PAYMENTS_HOST")
	paymentsPort := os.Getenv("PAYMENTS_PORT")
	paymentsAddress := fmt.Sprintf("%s:%s", paymentsHost, paymentsPort)
	authAddress := fmt.Sprintf("%s:%s", authHost, authPort)

	// Initialize auth connection
	logger.Infof("Connecting to Auth service at %s", authAddress)
	authConn, err := grpc.NewClient(
		authAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(
			otelgrpc.NewClientHandler(
				// Optional knobs:
				otelgrpc.WithTracerProvider(tp),
				otelgrpc.WithPropagators(propagation.TraceContext{}),
			// otelgrpc.WithMessageEvents(otelgrpc.ReceivedEvents, otelgrpc.SentEvents),
			),
		),
	)
	if err != nil {
		logger.Fatalf("Failed to connect to Auth service at %s: %v", authAddress, err)
	}
	defer func() {
		if err := authConn.Close(); err != nil {
			logger.Println("Failed to close auth connection:", err)
		}
	}()
	authClient := authpb.NewAuthServiceClient(authConn)
	logger.Info("Auth gRPC connection established.")

	// Initialize payments connection
	logger.Infof("Connecting to Payments service at %s", paymentsAddress)
	paymentsConn, err := grpc.NewClient(
		paymentsAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(
			otelgrpc.NewClientHandler(
				// Optional knobs:
				otelgrpc.WithTracerProvider(tp),
				otelgrpc.WithPropagators(propagation.TraceContext{}),
			// otelgrpc.WithMessageEvents(otelgrpc.ReceivedEvents, otelgrpc.SentEvents),
			),
		),
	)
	if err != nil {
		logger.Fatalf("Failed to connect to Payments service at %s: %v", paymentsAddress, err)
	}
	defer func() {
		if err := paymentsConn.Close(); err != nil {
			logger.Fatalf("Failed to close payments connection:", err)
		}
	}()
	logger.Info("Payments gRPC connection established.")
	paymentsClient := paymentspb.NewPaymentsServiceClient(paymentsConn)

	// Initialize application
	app := NewApplication(paymentsClient, authClient, tracer, logger)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /register", app.RegisterUser)
	mux.HandleFunc("POST /login", app.LoginUser)
	mux.HandleFunc("POST /create-account", app.CreateAccount)
	mux.HandleFunc("POST /customer/payment/authorize", app.CustomerPaymentAuthorize)
	mux.HandleFunc("POST /customer/payment/capture", app.CustomerPaymentCapture)
	mux.HandleFunc("POST /checkbalance", app.CheckBalance)
	logger.Info("API Gateway listening on port " + apiGatewayPort)
	err = http.ListenAndServe(":"+apiGatewayPort, withNotFoundHandler(mux))
	if err != nil {
		logger.Fatalf("Failed to start API Gateway server: %v", err)
	}
}
