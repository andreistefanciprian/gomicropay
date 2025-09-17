package email

import (
	"context"
	"fmt"
	"net/smtp"
	"os"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

var (
	emailSender   = os.Getenv("EMAIL_SENDER")
	emailPassword = os.Getenv("EMAIL_PASSWORD")
)

type Sender interface {
	SendEmail(ctx context.Context, customerEmail, merchantEmail, orderID string, amount int64) error
}

type EmailSender struct {
	Logger *logrus.Logger
	tracer trace.Tracer
}

func NewEmailSender(logger *logrus.Logger, tracer trace.Tracer) *EmailSender {
	return &EmailSender{
		Logger: logger,
		tracer: tracer,
	}
}

func (e *EmailSender) SendEmail(ctx context.Context, customerEmail, merchantEmail, orderID string, amount int64) error {
	_, span := e.tracer.Start(ctx, "SendEmail")
	defer span.End()

	e.Logger.Infof("Sending email to %s about order %s", customerEmail, orderID)
	subject := fmt.Sprintf("Purchase Order %s", orderID)
	body := fmt.Sprintf(
		"Purchase Order Notification\n\n"+
			"Order ID: %s\n"+
			"Customer: %s\n"+
			"Merchant: %s\n"+
			"Amount: %d cents\n\n"+
			"A transfer of %d cents has been recorded from the customer account (%s) to the merchant account (%s).\n",
		orderID, customerEmail, merchantEmail, amount, amount, customerEmail, merchantEmail,
	)

	msg := "From: " + emailSender + "\n" +
		"To: " + customerEmail + "\n" +
		"Subject: " + subject + "\n\n" +
		body

	err := smtp.SendMail("smtp.gmail.com:587",
		smtp.PlainAuth("", emailSender, emailPassword, "smtp.gmail.com"),
		emailSender, []string{customerEmail}, []byte(msg))

	if err != nil {
		e.Logger.Errorf("smtp error: %s", err)
		return err
	}

	e.Logger.Info("Purchase order email sent successfully!")
	return nil
}
