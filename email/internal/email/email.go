package email

import (
	"github.com/sirupsen/logrus"
)

type Sender interface {
	Send(emailAddress, orderID string) error
}

type EmailSender struct {
	Logger *logrus.Logger
}

func NewEmailSender(logger *logrus.Logger) *EmailSender {
	return &EmailSender{
		Logger: logger,
	}
}

func (e *EmailSender) Send(emailAddress, orderID string) error {
	// Implement actual email sending logic here
	// For now, just log the action
	e.Logger.Infof("Sending email to %s about order %s", emailAddress, orderID)
	return nil
}
