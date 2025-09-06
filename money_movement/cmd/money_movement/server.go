package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/IBM/sarama"
	"github.com/XSAM/otelsql"
	mm "github.com/andreistefanciprian/gomicropay/money_movement/internal/implementation"
	"github.com/andreistefanciprian/gomicropay/money_movement/internal/tracing"
	pb "github.com/andreistefanciprian/gomicropay/money_movement/proto"
	_ "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
)

const (
	dbDriver = "mysql"
)

var db *sql.DB

func main() {

	// Initialise tracing
	tp, err := tracing.InitTracer("money-movement")
	if err != nil {
		log.Fatalf("failed to initialize tracer: %v", err)
	}
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("money-movement-tracer")

	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	dbName := os.Getenv("MYSQL_DB")
	dbHost := os.Getenv("MYSQL_HOST")
	dbPort := os.Getenv("MYSQL_PORT")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)

	// Instrumented DB connection
	db, err := otelsql.Open("mysql", dsn, otelsql.WithTracerProvider(tp))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = db.Close(); err != nil {
			log.Printf("Error closing database connection: %v", err)
		}
		log.Println("Database connection closed")
	}()

	// Ping db
	if err = db.Ping(); err != nil {

		log.Fatalf("Failed to connect to the database: %v", err)
	} else {
		log.Println("Database connection established")
	}

	// Kafka producer setup
	saramaLogger := log.New(os.Stdout, "[money-movement producer]", log.LstdFlags)
	sarama.Logger = saramaLogger
	kafkaHost := os.Getenv("KAFKA_HOST")
	kafkaPort := os.Getenv("KAFKA_PORT")
	brokerAddr := fmt.Sprintf("%s:%s", kafkaHost, kafkaPort)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Println("Error closing Kafka producer:", err)
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
	pb.RegisterMoneyMovementServiceServer(grpcServer, mm.NewMoneyMovementImplementation(db, producer, tracer))

	// listen and serve
	moneyMovementPort := os.Getenv("MONEY_MOVEMENT_PORT")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", moneyMovementPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("gRPC server listening at %v", listener.Addr())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
