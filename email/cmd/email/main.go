package main

import (
	"log"
	"os"
	"sync"

	"context"

	"github.com/IBM/sarama"
	c "github.com/andreistefanciprian/gomicropay/email/internal/consumer"
	"github.com/andreistefanciprian/gomicropay/email/internal/email"
	"github.com/andreistefanciprian/gomicropay/email/internal/tracing"
	"github.com/sirupsen/logrus"
)

const topic = "email"

var wg sync.WaitGroup

var logger = logrus.New()

func initLogger() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)
	logger.SetFormatter(&logrus.TextFormatter{
		DisableColors: false,
	})
	logger.Infof("Logger initialized with log level: %s", logLevel)
}

func main() {

	// Initialize logger
	initLogger()

	// Initialise tracing
	tp, err := tracing.InitTracer("email")
	if err != nil {
		logger.Fatalf("failed to initialize tracer: %v", err)
	}
	defer tp.Shutdown(context.Background())
	tracer := tp.Tracer("email-tracer")

	// Initialize email sender

	emailSender := email.NewEmailSender(logger, tracer)

	// Initialize message consumer
	messageConsumer := c.NewMessageConsumer(tracer, emailSender, logger)

	logger.Infof("Starting email consumer")

	// Initialize Kafka consumer
	kafkaHost := os.Getenv("KAFKA_HOST")
	kafkaPort := os.Getenv("KAFKA_PORT")
	kafkaAddr := kafkaHost + ":" + kafkaPort

	sarama.Logger = log.New(os.Stdout, "[email-consumer]", log.LstdFlags)

	done := make(chan struct{})

	kafkaConsumer, err := sarama.NewConsumer([]string{kafkaAddr}, sarama.NewConfig())
	if err != nil {
		logger.Fatal("Error creating consumer:", err)
	}
	defer func() {
		close(done)
		if err := kafkaConsumer.Close(); err != nil {
			logger.Error("Error closing consumer:", err)
		}
	}()

	partitions, err := kafkaConsumer.Partitions(topic)
	if err != nil {
		logger.Fatal(err)
	}

	for _, partition := range partitions {
		logger.Infof("Starting consumer for partition %d", partition)
		partitionConsumer, err := kafkaConsumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			logger.Fatal("Error creating partition consumer:", err)
		}
		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				logger.Error("Error closing partition consumer:", err)
			}
		}()

		wg.Add(1)
		go messageConsumer.ConsumeMessages(partitionConsumer, partition, done, &wg)
	}

	wg.Wait()
}
