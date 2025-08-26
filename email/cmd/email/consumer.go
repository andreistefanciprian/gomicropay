package email

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/email/internal/email"
)

const topic = "email"

var wg sync.WaitGroup

type EmailMsg struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
}

func main() {
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
	var emailMsg EmailMsg
	if err := json.Unmarshal(msg.Value, &emailMsg); err != nil {
		log.Println("Error unmarshalling message:", err)
		return
	}
	err := email.Send(emailMsg.UserID, emailMsg.OrderID)
	if err != nil {
		log.Println("Error sending email:", err)
		return
	}
}
