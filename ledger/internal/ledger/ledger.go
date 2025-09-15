package ledger

import (
	"context"
	"database/sql"

	"go.opentelemetry.io/otel/trace"
)

const insertQuery = "INSERT INTO ledger (order_id, customer_email_address, amount, operation, transaction_date) VALUES (?, ?, ?, ?, ?)"

// Insert a new ledger entry into the database
func Insert(ctx context.Context, tracer trace.Tracer, db *sql.DB, orderID, customerEmailAddress string, amount int64, operation, transactionDate string) error {

	ctx, span := tracer.Start(ctx, "InsertLedgerEntry")
	defer span.End()

	stmt, err := db.PrepareContext(ctx, insertQuery)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, orderID, customerEmailAddress, amount, operation, transactionDate)
	if err != nil {
		return err
	}

	return nil
}
