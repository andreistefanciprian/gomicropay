package producer

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	emailTopic  = "email"
	ledgerTopic = "ledger"
)

type EmailMsg struct {
	OrderID      string `json:"order_id"`
	EmailAddress string `json:"email_address"`
}

type LedgerMsg struct {
	OrderID              string `json:"order_id"`
	CustomerEmailAddress string `json:"customer_email_address"`
	Amount               int64  `json:"amount"`
	Operation            string `json:"operation"`
	Date                 string `json:"date"`
}

func SendCaptureMessage(producer sarama.SyncProducer, pid, customerEmailAddress string, amount int64) {
	// Create email message
	emailMsg := EmailMsg{
		OrderID:      pid,
		EmailAddress: customerEmailAddress,
	}

	ledgerMsg := LedgerMsg{
		OrderID:              pid,
		CustomerEmailAddress: customerEmailAddress,
		Amount:               amount,
		Operation:            "DEBIT",
		Date:                 time.Now().Format("2006-01-02"),
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go sendMsg(producer, emailMsg, emailTopic, &wg)
	go sendMsg(producer, ledgerMsg, ledgerTopic, &wg)
	wg.Wait()
}

func sendMsg[T EmailMsg | LedgerMsg](producer sarama.SyncProducer, msg T, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	stringMsg, err := json.Marshal(msg)
	if err != nil {
		log.Println("Error marshalling message:", err)
		return
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(stringMsg),
	}

	//  Send message to Kafka topic
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Println("Error sending message to topic:", err)
		return
	}

	log.Printf("Message sent to topic %s at partition %d and offset %d\n", topic, partition, offset)
}
