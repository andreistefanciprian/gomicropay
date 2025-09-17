package producer

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	emailTopic  = "email"
	ledgerTopic = "ledger"
)

type MessageProducer struct {
	producer sarama.SyncProducer
	tracer   trace.Tracer
	logger   *logrus.Logger
}

var kafkaPropagator = propagation.TraceContext{}

func NewMessageProducer(producer sarama.SyncProducer, tracer trace.Tracer, logger *logrus.Logger) *MessageProducer {
	return &MessageProducer{
		producer: producer,
		tracer:   tracer,
		logger:   logger,
	}
}

type EmailMsg struct {
	OrderID              string `json:"order_id"`
	CustomerEmailAddress string `json:"customer_email_address"`
	Amount               int64  `json:"amount"`
	MerchantEmailAddress string `json:"merchant_email_address"`
	Date                 string `json:"date"`
}

type LedgerMsg struct {
	OrderID              string `json:"order_id"`
	CustomerEmailAddress string `json:"customer_email_address"`
	Amount               int64  `json:"amount"`
	Operation            string `json:"operation"`
	Date                 string `json:"date"`
}

func (k *MessageProducer) ProduceMessage(ctx context.Context, pid, customerEmailAddress, merchantEmailAddress string, amount int64) {
	// Start a new span for tracing
	ctx, span := k.tracer.Start(ctx, "ProduceMessage")
	defer span.End()

	timeNow := time.Now().Format("2006-01-02")
	// Create email message
	emailMsg := EmailMsg{
		OrderID:              pid,
		CustomerEmailAddress: customerEmailAddress,
		Amount:               amount,
		MerchantEmailAddress: merchantEmailAddress,
		Date:                 timeNow,
	}

	ledgerMsg := LedgerMsg{
		OrderID:              pid,
		CustomerEmailAddress: customerEmailAddress,
		Amount:               amount,
		Operation:            "DEBIT",
		Date:                 timeNow,
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go k.SendEmailMsg(ctx, emailMsg, emailTopic, &wg)
	go k.SendLedgerMsg(ctx, ledgerMsg, ledgerTopic, &wg)
	wg.Wait()
}

func (k *MessageProducer) SendEmailMsg(ctx context.Context, msg EmailMsg, topic string, wg *sync.WaitGroup) {
	// Start a new span for tracing
	ctx, span := k.tracer.Start(ctx, "SendEmailMsg")
	defer span.End()

	defer wg.Done()
	stringMsg, err := json.Marshal(msg)
	if err != nil {
		k.logger.Error("Error marshalling message: ", err)
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
		k.logger.Error("Error sending message to topic: ", err)
		return
	}

	k.logger.Infof("Message sent to topic %s at partition %d and offset %d", topic, partition, offset)
}

func (k *MessageProducer) SendLedgerMsg(ctx context.Context, msg LedgerMsg, topic string, wg *sync.WaitGroup) {
	// Start a new span for tracing
	ctx, span := k.tracer.Start(ctx, "SendLedgerMsg")
	defer span.End()

	defer wg.Done()
	stringMsg, err := json.Marshal(msg)
	if err != nil {
		k.logger.Error("Error marshalling message: ", err)
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
		k.logger.Error("Error sending message to topic: ", err)
		return
	}

	k.logger.Infof("Message sent to topic %s at partition %d and offset %d", topic, partition, offset)
}
