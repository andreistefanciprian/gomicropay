package email

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

type Sender interface {
	SendEmail(ctx context.Context, emailAddress, orderID string) error
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

func (e *EmailSender) SendEmail(ctx context.Context, emailAddress, orderID string) error {
	_, span := e.tracer.Start(ctx, "SendEmail")
	defer span.End()

	// Implement actual email sending logic here
	// For now, just log the action.
	e.Logger.Infof("Sending email to %s about order %s", emailAddress, orderID)
	return nil
}
