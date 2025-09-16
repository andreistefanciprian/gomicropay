package email

import (
	"context"

	"github.com/sirupsen/logrus"
)

type Sender interface {
	Send(ctx context.Context, emailAddress, orderID string) error
}

type EmailSender struct {
	Logger *logrus.Logger
}

func NewEmailSender(logger *logrus.Logger) *EmailSender {
	return &EmailSender{
		Logger: logger,
	}
}

func (e *EmailSender) Send(ctx context.Context, emailAddress, orderID string) error {
	// Implement actual email sending logic here
	// For now, just log the action. Use ctx for tracing in the future.
	e.Logger.Infof("Sending email to %s about order %s", emailAddress, orderID)
	return nil
}
