package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/andreistefanciprian/gomicropay/auth/internal/db"
	"github.com/andreistefanciprian/gomicropay/auth/internal/implementation/auth"
	"github.com/andreistefanciprian/gomicropay/auth/internal/tracing"
	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
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
	tp, err := tracing.InitTracer("auth")
	if err != nil {
		logger.Fatalf("failed to initialize tracer: %v", err)
	}
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("auth-tracer")

	// Initialize DB connection
	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	dbName := os.Getenv("MYSQL_DB")
	dbHost := os.Getenv("MYSQL_HOST")
	dbPort := os.Getenv("MYSQL_PORT")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
	mySql, err := db.NewMysqlDb("mysql", dsn, tp, logger)
	if err != nil {
		logger.Fatal(err)
	}
	defer mySql.Close()

	// Initialize gRPC server setup
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

	authServerImplementation := auth.NewAuthImplementation(mySql, tracer, logger)
	pb.RegisterAuthServiceServer(grpcServer, authServerImplementation)

	// listen and serve
	authPort := os.Getenv("AUTH_SERVICE_PORT")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", authPort))
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}
	if err := grpcServer.Serve(listener); err != nil {
		logger.Fatalf("Failed to serve gRPC server: %v", err)
	}
	logger.Infof("gRPC server listening at %v", listener.Addr())
}
