package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
	"github.com/XSAM/otelsql"
	"github.com/andreistefanciprian/gomicropay/ledger/internal/ledger"
	"github.com/andreistefanciprian/gomicropay/ledger/internal/tracing"
	_ "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	dbDriver = "mysql"
	topic    = "ledger"
)

var logLevel string

func logDebug(format string, v ...interface{}) {
	if logLevel == "DEBUG" {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func logInfo(format string, v ...interface{}) {
	if logLevel == "INFO" || logLevel == "DEBUG" {
		log.Printf("[INFO] "+format, v...)
	}
}

var (
	db *sql.DB
	wg sync.WaitGroup
)

type LedgerMsg struct {
	OrderID              string `json:"order_id"`
	CustomerEmailAddress string `json:"customer_email_address"`
	Amount               int64  `json:"amount"`
	Operation            string `json:"operation"`
	Date                 string `json:"date"`
}

func main() {

	// Initialise tracing
	tp, err := tracing.InitTracer("ledger")
	if err != nil {
		log.Fatalf("failed to initialize tracer: %v", err)
	}
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("ledger-tracer")

	logLevel = os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "INFO"
	}
	logInfo("Starting ledger consumer with log level: %s", logLevel)
	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	dbName := os.Getenv("MYSQL_DB")
	dbHost := os.Getenv("MYSQL_HOST")
	dbPort := os.Getenv("MYSQL_PORT")
	kafkaHost := os.Getenv("KAFKA_HOST")
	kafkaPort := os.Getenv("KAFKA_PORT")
	brokerAddr := fmt.Sprintf("%s:%s", kafkaHost, kafkaPort)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)

	// DB connection
	// Instrumented DB connection
	db, err = otelsql.Open("mysql", dsn, otelsql.WithTracerProvider(tp))
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

	done := make(chan struct{})
	sarama.Logger = log.New(os.Stdout, "[ledger-consumer]", log.LstdFlags)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	consumer, err := sarama.NewConsumer([]string{brokerAddr}, config)
	if err != nil {
		log.Fatal("Error creating ledger consumer:", err)
	}
	defer func() {
		close(done)
		if err := consumer.Close(); err != nil {
			log.Println("Error closing ledger consumer:", err)
		}
	}()

	ledgerConsumer := NewLedgerConsumer(tracer, db, consumer)

	partitions, err := ledgerConsumer.consumer.Partitions(topic)
	if err != nil {
		log.Fatal(err)
	}

	for _, partition := range partitions {
		logInfo("Starting ledger consumer for partition %d", partition)
		partitionConsumer, err := ledgerConsumer.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatal("Error creating partition consumer:", err)
		}
		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				log.Println("Error closing partition consumer:", err)
			}
		}()

		wg.Add(1)
		go ledgerConsumer.awaitMessages(partitionConsumer, partition, done)
	}

	wg.Wait()
}

type LedgerConsumer struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	db         *sql.DB
	consumer   sarama.Consumer
}

func NewLedgerConsumer(tracer trace.Tracer, db *sql.DB, consumer sarama.Consumer) *LedgerConsumer {
	return &LedgerConsumer{
		tracer:     tracer,
		propagator: propagation.TraceContext{},
		db:         db,
		consumer:   consumer,
	}
}

func (l *LedgerConsumer) awaitMessages(partitionConsumer sarama.PartitionConsumer, partition int32, done chan struct{}) {
	defer wg.Done()
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			logInfo("Partition %d: Received message", partition)
			logDebug("Partition %d: Message body: %s", partition, string(msg.Value))
			l.handleMessage(msg)
		case <-done:
			logInfo("Received Done signal. Partition %d: Shutting down consumer", partition)
			return
		}
	}
}

func (l *LedgerConsumer) handleMessage(msg *sarama.ConsumerMessage) {
	// Extract trace context from Kafka headers
	carrier := propagation.MapCarrier{}
	for _, h := range msg.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}
	ctx := context.Background()
	ctx = l.propagator.Extract(ctx, carrier)

	ctx, span := l.tracer.Start(ctx, "handleMessage")
	defer span.End()

	var ledgerlMsg LedgerMsg
	if err := json.Unmarshal(msg.Value, &ledgerlMsg); err != nil {
		logInfo("Error unmarshalling message: %v", err)
		span.RecordError(err)
		return
	}
	logDebug("LedgerMsg unmarshalled: %+v", ledgerlMsg)
	err := ledger.Insert(ctx, l.tracer, db, ledgerlMsg.OrderID, ledgerlMsg.CustomerEmailAddress, ledgerlMsg.Amount, ledgerlMsg.Operation, ledgerlMsg.Date)
	if err != nil {
		logInfo("Error inserting ledger message: %v", err)
		span.RecordError(err)
		return
	}
	logInfo("Ledger message inserted for order %s, customer email %s", ledgerlMsg.OrderID, ledgerlMsg.CustomerEmailAddress)
}
