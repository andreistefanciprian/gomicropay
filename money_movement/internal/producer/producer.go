package producer

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	emailTopic  = "email"
	ledgerTopic = "ledger"
)

type KafkaProducer struct {
	producer sarama.SyncProducer
	tracer   trace.Tracer
}

var kafkaPropagator = propagation.TraceContext{}

func NewKafkaProducer(producer sarama.SyncProducer, tracer trace.Tracer) *KafkaProducer {
	return &KafkaProducer{
		producer: producer,
		tracer:   tracer,
	}
}

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

func (k *KafkaProducer) SendCaptureMessage(ctx context.Context, pid, customerEmailAddress string, amount int64) {
	// Start a new span for tracing
	ctx, span := k.tracer.Start(ctx, "SendCaptureMessage")
	defer span.End()

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
	go k.SendEmailMsg(ctx, emailMsg, emailTopic, &wg)
	go k.SendLedgerMsg(ctx, ledgerMsg, ledgerTopic, &wg)
	wg.Wait()
}

func (k *KafkaProducer) SendEmailMsg(ctx context.Context, msg EmailMsg, topic string, wg *sync.WaitGroup) {
	// Start a new span for tracing
	_, span := k.tracer.Start(ctx, "SendEmailMsg")
	defer span.End()

	defer wg.Done()
	stringMsg, err := json.Marshal(msg)
	if err != nil {
		log.Println("Error marshalling message:", err)
		return
	}

	// Inject trace context into Kafka headers
	kafkaHeaders := []sarama.RecordHeader{}
	carrier := propagation.MapCarrier{}
	kafkaPropagator.Inject(ctx, &carrier)
	for k, v := range carrier {
		kafkaHeaders = append(kafkaHeaders, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}

	message := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.StringEncoder(stringMsg),
		Headers: kafkaHeaders,
	}

	//  Send message to Kafka topic
	partition, offset, err := k.producer.SendMessage(message)
	if err != nil {
		log.Println("Error sending message to topic:", err)
		return
	}

	log.Printf("Message sent to topic %s at partition %d and offset %d\n", topic, partition, offset)

}

func (k *KafkaProducer) SendLedgerMsg(ctx context.Context, msg LedgerMsg, topic string, wg *sync.WaitGroup) {
	// Start a new span for tracing
	_, span := k.tracer.Start(ctx, "SendLedgerMsg")
	defer span.End()

	defer wg.Done()
	stringMsg, err := json.Marshal(msg)
	if err != nil {
		log.Println("Error marshalling message:", err)
		return
	}

	// Inject trace context into Kafka headers
	kafkaHeaders := []sarama.RecordHeader{}
	carrier := propagation.MapCarrier{}
	kafkaPropagator.Inject(ctx, &carrier)
	for k, v := range carrier {
		kafkaHeaders = append(kafkaHeaders, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}

	message := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.StringEncoder(stringMsg),
		Headers: kafkaHeaders,
	}

	//  Send message to Kafka topic
	partition, offset, err := k.producer.SendMessage(message)
	if err != nil {
		log.Println("Error sending message to topic:", err)
		return
	}

	log.Printf("Message sent to topic %s at partition %d and offset %d\n", topic, partition, offset)
}

// func sendMsg[T EmailMsg | LedgerMsg](ctx context.Context, producer sarama.SyncProducer, msg T, topic string, wg *sync.WaitGroup) {
// 	// Start a new span for tracing
// 	ctx, span := k.tracer.Start(ctx, "SendMsg")
// 	defer span.End()

// 	defer wg.Done()
// 	stringMsg, err := json.Marshal(msg)
// 	if err != nil {
// 		log.Println("Error marshalling message:", err)
// 		return
// 	}

// 	message := &sarama.ProducerMessage{
// 		Topic: topic,
// 		Value: sarama.StringEncoder(stringMsg),
// 	}

// 	//  Send message to Kafka topic
// 	partition, offset, err := producer.SendMessage(message)
// 	if err != nil {
// 		log.Println("Error sending message to topic:", err)
// 		return
// 	}

// 	log.Printf("Message sent to topic %s at partition %d and offset %d\n", topic, partition, offset)
// }
