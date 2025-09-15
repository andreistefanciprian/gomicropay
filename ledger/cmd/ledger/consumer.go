package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/ledger/internal/db"
	"github.com/andreistefanciprian/gomicropay/ledger/internal/ledger"
	"github.com/andreistefanciprian/gomicropay/ledger/internal/tracing"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

const (
	dbDriver = "mysql"
	topic    = "ledger"
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
	logger.Infof("Logger initialized with log level: %s", logLevel)
}

var (
	wg sync.WaitGroup
)

func main() {

	// Initialise tracing
	tp, err := tracing.InitTracer("ledger")
	if err != nil {
		logger.Fatalf("failed to initialize tracer: %v", err)
	}
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("ledger-tracer")

	// Initialize logger
	initLogger()

	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	dbName := os.Getenv("MYSQL_DB")
	dbHost := os.Getenv("MYSQL_HOST")
	dbPort := os.Getenv("MYSQL_PORT")
	kafkaHost := os.Getenv("KAFKA_HOST")
	kafkaPort := os.Getenv("KAFKA_PORT")
	brokerAddr := fmt.Sprintf("%s:%s", kafkaHost, kafkaPort)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)

	// Initialise DB
	mySqlConn, err := db.NewMysqlDb(dbDriver, dsn, tp, logger)
	if err != nil {
		logger.Fatal(err)
	}
	defer mySqlConn.Close()

	// Kafka consumer setup
	done := make(chan struct{})
	sarama.Logger = log.New(os.Stdout, "[ledger-consumer]", log.LstdFlags)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	consumer, err := sarama.NewConsumer([]string{brokerAddr}, config)
	if err != nil {
		logger.Fatal("Error creating ledger consumer:", err)
	}
	defer func() {
		close(done)
		if err := consumer.Close(); err != nil {
			logger.Error("Error closing ledger consumer: ", err)
		}
	}()

	ledgerConsumer := ledger.NewLedgerConsumer(tracer, mySqlConn, consumer, logger)

	partitions, err := ledgerConsumer.Consumer.Partitions(topic)
	if err != nil {
		logger.Fatal(err)
	}

	for _, partition := range partitions {
		logger.Infof("Starting ledger consumer for partition %d", partition)
		partitionConsumer, err := ledgerConsumer.Consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			logger.Fatal("Error creating partition consumer:", err)
		}
		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				logger.Error("Error closing partition consumer: ", err)
			}
		}()

		wg.Add(1)
		go ledgerConsumer.AwaitMessages(partitionConsumer, partition, &wg, done)
	}
	wg.Wait()
}
