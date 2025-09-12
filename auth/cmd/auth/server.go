package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/andreistefanciprian/gomicropay/auth/internal/implementation/auth"
	"github.com/andreistefanciprian/gomicropay/auth/internal/implementation/db"
	"github.com/andreistefanciprian/gomicropay/auth/internal/tracing"
	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
	_ "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
)

func main() {
	// Initialise tracing
	tp, err := tracing.InitTracer("auth")
	if err != nil {
		log.Fatalf("failed to initialize tracer: %v", err)
	}
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("auth-tracer")

	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	dbName := os.Getenv("MYSQL_DB")
	dbHost := os.Getenv("MYSQL_HOST")
	dbPort := os.Getenv("MYSQL_PORT")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)

	// Instrumented DB connection
	mySql, err := db.NewMysqlDb("mysql", dsn, tp)
	if err != nil {
		log.Fatal(err)
	}

	defer mySql.Close()

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

	authServerImplementation := auth.NewAuthImplementation(mySql, tracer)
	pb.RegisterAuthServiceServer(grpcServer, authServerImplementation)

	// listen and serve
	authPort := os.Getenv("AUTH_SERVICE_PORT")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", authPort))
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
	log.Printf("gRPC server listening at %v", listener.Addr())
}
