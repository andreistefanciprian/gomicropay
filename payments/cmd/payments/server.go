package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/payments/internal/db"
	p "github.com/andreistefanciprian/gomicropay/payments/internal/implementation"
	"github.com/andreistefanciprian/gomicropay/payments/internal/tracing"
	pb "github.com/andreistefanciprian/gomicropay/payments/proto"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
)

var logger = logrus.New()

func initLogger() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)
	logger.SetFormatter(&logrus.TextFormatter{
		DisableColors: false,
		PadLevelText:  true,
	})
	logger.Infof("Logger initialized with log level: %s", logLevel)
}

func main() {

	// Initialize logger
	initLogger()

	// Initialise tracing
	tp, err := tracing.InitTracer("payments")
	if err != nil {
		logger.Fatalf("failed to initialize tracer: %v", err)
	}
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("payments-tracer")

	// Initialize DB connection
	db, err := db.NewMysqlDb(tp, logger)
	if err != nil {
		logger.Fatalf("failed to initialize database: %v", err)
	}
	defer db.Close()

	// Kafka producer setup
	saramaLogger := log.New(os.Stdout, "[payments producer]", log.LstdFlags)
	sarama.Logger = saramaLogger
	kafkaHost := os.Getenv("KAFKA_HOST")
	kafkaPort := os.Getenv("KAFKA_PORT")
	brokerAddr := fmt.Sprintf("%s:%s", kafkaHost, kafkaPort)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		logger.Fatalf("Error creating Kafka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("Error closing Kafka producer:", err)
		}
	}()

	// Instrumented gRPC server setup
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(
			otelgrpc.NewServerHandler(
				// optional but nice if you have a custom provider/propagators:
				otelgrpc.WithTracerProvider(tp),
				otelgrpc.WithPropagators(propagation.TraceContext{}),
			// otelgrpc.WithFilter(func(ctx context.Context, info otelgrpc.InterceptorInfo) bool { return true }),
			),
		),
		// you can still chain your own unary interceptors for recovery, auth, etc.
		// grpc.ChainUnaryInterceptor(recoveryUnaryServerInterceptor()),
	)
	pb.RegisterPaymentsServiceServer(grpcServer, p.NewPaymentsImplementation(db, producer, tracer, logger))

	// listen and serve
	paymentsPort := os.Getenv("PAYMENTS_PORT")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", paymentsPort))
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}
	logger.Infof("gRPC server listening at %v", listener.Addr())
	if err := grpcServer.Serve(listener); err != nil {
		logger.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
