package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/IBM/sarama"
	mm "github.com/andreistefanciprian/gomicropay/money_movement/internal/implementation"
	pb "github.com/andreistefanciprian/gomicropay/money_movement/proto"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
)

const (
	dbDriver = "mysql"
)

var db *sql.DB

func main() {

	var err error
	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	dbName := os.Getenv("MYSQL_DB")
	dbHost := os.Getenv("MYSQL_HOST")
	dbPort := os.Getenv("MYSQL_PORT")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)

	db, err = sql.Open(dbDriver, dsn)
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

	// gRPC server setup
	grpcServer := grpc.NewServer()
	pb.RegisterMoneyMovementServiceServer(grpcServer, mm.NewMoneyMovementImplementation(db, producer))
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
