package ledger

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/IBM/sarama"
	"github.com/andreistefanciprian/gomicropay/ledger/internal/db"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type LedgerMsg struct {
	OrderID              string `json:"order_id"`
	CustomerEmailAddress string `json:"customer_email_address"`
	Amount               int64  `json:"amount"`
	Operation            string `json:"operation"`
	Date                 string `json:"date"`
}

type LedgerConsumer struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	db         db.LedgerRepository
	Consumer   sarama.Consumer
	logger     *logrus.Logger
}

func NewLedgerConsumer(tracer trace.Tracer, db db.LedgerRepository, consumer sarama.Consumer, logger *logrus.Logger) *LedgerConsumer {
	return &LedgerConsumer{
		tracer:     tracer,
		propagator: propagation.TraceContext{},
		db:         db,
		Consumer:   consumer,
		logger:     logger,
	}
}

func (l *LedgerConsumer) AwaitMessages(partitionConsumer sarama.PartitionConsumer, partition int32, wg *sync.WaitGroup, done chan struct{}) {
	defer wg.Done()
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			l.logger.Infof("Partition %d: Received message", partition)
			l.consumeMessage(msg)
		case <-done:
			l.logger.Infof("Received Done signal. Partition %d: Shutting down consumer", partition)
			return
		}
	}
}

func (l *LedgerConsumer) consumeMessage(msg *sarama.ConsumerMessage) {
	// Extract trace context from Kafka headers
	carrier := propagation.MapCarrier{}
	for _, h := range msg.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}
	ctx := context.Background()
	ctx = l.propagator.Extract(ctx, carrier)

	ctx, span := l.tracer.Start(ctx, "consumeMessage")
	defer span.End()

	var ledgerlMsg LedgerMsg
	if err := json.Unmarshal(msg.Value, &ledgerlMsg); err != nil {
		l.logger.Error("Error unmarshalling message: ", err)
		span.RecordError(err)
		return
	}
	l.logger.Debugf("LedgerMsg unmarshalled: %+v", ledgerlMsg)
	err := l.db.Insert(ctx, ledgerlMsg.OrderID, ledgerlMsg.CustomerEmailAddress, ledgerlMsg.Amount, ledgerlMsg.Operation, ledgerlMsg.Date)
	if err != nil {
		l.logger.Error("Error inserting ledger message: ", err)
		span.RecordError(err)
		return
	}
	l.logger.Infof("Ledger message inserted for order %s, customer email %s", ledgerlMsg.OrderID, ledgerlMsg.CustomerEmailAddress)
}
