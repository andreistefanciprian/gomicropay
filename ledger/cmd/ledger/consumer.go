package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/ledger/internal/ledger"
)

const (
	dbDriver   = "mysql"
	dbUser     = "root"
	dbPassword = "Admin123"
	dbName     = "ledger"
	topic      = "ledger"
)

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
	dsn := fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s", dbUser, dbPassword, dbName)
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
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, sarama.NewConfig())
	if err != nil {
		log.Fatal("Error creating consumer:", err)
	}
	defer func() {
		close(done)
		if err := consumer.Close(); err != nil {
			log.Println("Error closing consumer:", err)
		}
	}()

	partitions, err := consumer.partitions(topic)
	if err != nil {
		log.Fatal(err)
	}

	for _, partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
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
	// close(done)
}

func awaitMessages(partitionConsumer sarama.PartitionConsumer, partition int32, done chan struct{}) {
	defer wg.Done()
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Partition %d: Received message: %s\n", partition, string(msg.Value))
			handleMessage(msg)
		case <-done:
			log.Printf("Received Done signal.Partition %d: Shutting down consumer\n", partition)
			return
		}
	}
}

func handleMessage(msg *sarama.ConsumerMessage) {
	var ledgerlMsg LedgerMsg
	if err := json.Unmarshal(msg.Value, &ledgerlMsg); err != nil {
		log.Println("Error unmarshalling message:", err)
		return
	}
	err := ledger.Insert(db, ledgerlMsg.OrderID, ledgerlMsg.UserID, ledgerlMsg.Amount, ledgerlMsg.Operation, ledgerlMsg.Date)
	if err != nil {
		log.Println("Error inserting ledger message:", err)
		return
	}
}
