package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/ledger/internal/ledger"
	_ "github.com/go-sql-driver/mysql"
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
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
	Operation string `json:"operation"`
	Date      string `json:"date"`
}

func main() {

	var err error
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

	done := make(chan struct{})
	sarama.Logger = log.New(os.Stdout, "[ledger-consumer]", log.LstdFlags)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	ledgerConsumer, err := sarama.NewConsumer([]string{brokerAddr}, config)
	if err != nil {
		log.Fatal("Error creating ledger consumer:", err)
	}
	defer func() {
		close(done)
		if err := ledgerConsumer.Close(); err != nil {
			log.Println("Error closing ledger consumer:", err)
		}
	}()

	partitions, err := ledgerConsumer.Partitions(topic)
	if err != nil {
		log.Fatal(err)
	}

	for _, partition := range partitions {
		logInfo("Starting ledger consumer for partition %d", partition)
		partitionConsumer, err := ledgerConsumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatal("Error creating partition consumer:", err)
		}
		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				log.Println("Error closing partition consumer:", err)
			}
		}()

		wg.Add(1)
		go awaitMessages(partitionConsumer, partition, done)
	}

	wg.Wait()
}

func awaitMessages(partitionConsumer sarama.PartitionConsumer, partition int32, done chan struct{}) {
	defer wg.Done()
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			logInfo("Partition %d: Received message", partition)
			logDebug("Partition %d: Message body: %s", partition, string(msg.Value))
			handleMessage(msg)
		case <-done:
			logInfo("Received Done signal. Partition %d: Shutting down consumer", partition)
			return
		}
	}
}

func handleMessage(msg *sarama.ConsumerMessage) {
	var ledgerlMsg LedgerMsg
	if err := json.Unmarshal(msg.Value, &ledgerlMsg); err != nil {
		logInfo("Error unmarshalling message: %v", err)
		return
	}
	logDebug("LedgerMsg unmarshalled: %+v", ledgerlMsg)
	err := ledger.Insert(db, ledgerlMsg.OrderID, ledgerlMsg.UserID, ledgerlMsg.Amount, ledgerlMsg.Operation, ledgerlMsg.Date)
	if err != nil {
		logInfo("Error inserting ledger message: %v", err)
		return
	}
	logInfo("Ledger message inserted for order %s, user %s", ledgerlMsg.OrderID, ledgerlMsg.UserID)
}
