package consumer

import (
	"encoding/json"
	"sync"

	"context"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/email/internal/email"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type EmailMsg struct {
	OrderID      string `json:"order_id"`
	EmailAddress string `json:"email_address"`
}

type MessageConsumer struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	email      email.Sender
	logger     *logrus.Logger
}

func NewMessageConsumer(tracer trace.Tracer, emailService email.Sender, logger *logrus.Logger) *MessageConsumer {
	return &MessageConsumer{
		tracer:     tracer,
		email:      emailService,
		propagator: propagation.TraceContext{},
		logger:     logger,
	}
}

func (mc *MessageConsumer) ConsumeMessages(partitionConsumer sarama.PartitionConsumer, partition int32, done chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			mc.logger.Infof("Partition %d: Received message", partition)
			mc.logger.Debugf("Partition %d: Message body: %s", partition, string(msg.Value))
			mc.processMessage(msg)
		case <-done:
			mc.logger.Infof("Received Done signal. Partition %d: Shutting down consumer", partition)
			return
		}
	}
}

func (mc *MessageConsumer) processMessage(msg *sarama.ConsumerMessage) {
	// Extract trace context from Kafka headers
	carrier := propagation.MapCarrier{}
	for _, h := range msg.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}
	ctx := context.Background()
	ctx = mc.propagator.Extract(ctx, carrier)

	ctx, span := mc.tracer.Start(ctx, "processMessage")
	defer span.End()

	var emailMsg EmailMsg
	if err := json.Unmarshal(msg.Value, &emailMsg); err != nil {
		mc.logger.Error("Error unmarshalling message: ", err)
		span.RecordError(err)
		return
	}
	mc.logger.Debugf("EmailMsg unmarshalled: %+v", emailMsg)
	err := mc.email.SendEmail(ctx, emailMsg.EmailAddress, emailMsg.OrderID)
	if err != nil {
		mc.logger.Error("Error sending email: ", err)
		span.RecordError(err)
		return
	}
	mc.logger.Infof("Email sent to user %s for order %s", emailMsg.EmailAddress, emailMsg.OrderID)
}
