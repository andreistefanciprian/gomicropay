package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/email/internal/email"
)

const topic = "email"

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

var wg sync.WaitGroup

type EmailMsg struct {
	OrderID      string `json:"order_id"`
	EmailAddress string `json:"email_address"`
}

func main() {
	logLevel = os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "INFO"
	}
	logInfo("Starting email consumer with log level: %s", logLevel)

	kafkaHost := os.Getenv("KAFKA_HOST")
	kafkaPort := os.Getenv("KAFKA_PORT")
	kafkaAddr := kafkaHost + ":" + kafkaPort

	sarama.Logger = log.New(os.Stdout, "[email-consumer]", log.LstdFlags)

	done := make(chan struct{})
	consumer, err := sarama.NewConsumer([]string{kafkaAddr}, sarama.NewConfig())
	if err != nil {
		log.Fatal("Error creating consumer:", err)
	}
	defer func() {
		close(done)
		if err := consumer.Close(); err != nil {
			log.Println("Error closing consumer:", err)
		}
	}()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatal(err)
	}

	for _, partition := range partitions {
		logInfo("Starting consumer for partition %d", partition)
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
	var emailMsg EmailMsg
	if err := json.Unmarshal(msg.Value, &emailMsg); err != nil {
		logInfo("Error unmarshalling message: %v", err)
		return
	}
	logDebug("EmailMsg unmarshalled: %+v", emailMsg)
	err := email.Send(emailMsg.EmailAddress, emailMsg.OrderID)
	if err != nil {
		logInfo("Error sending email: %v", err)
		return
	}
	logInfo("Email sent to user %s for order %s", emailMsg.EmailAddress, emailMsg.OrderID)
}
