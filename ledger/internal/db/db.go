package db

import (
	"context"
	"database/sql"

	"github.com/XSAM/otelsql"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/sdk/trace"
)

const insertQuery = "INSERT INTO ledger (order_id, customer_email_address, amount, operation, transaction_date) VALUES (?, ?, ?, ?, ?)"

type LedgerRepository interface {
	Insert(ctx context.Context, orderID, customerEmailAddress string, amount int64, operation, transactionDate string) error
}

type MySqlDb struct {
	DB     *sql.DB
	logger *logrus.Logger
}

// NewMysqlDb creates a new MySqlDb instance with an instrumented DB connection
func NewMysqlDb(dbName string, dsn string, trace *trace.TracerProvider, logger *logrus.Logger) (*MySqlDb, error) {
	db, err := otelsql.Open(dbName, dsn, otelsql.WithTracerProvider(trace))
	if err != nil {
		return &MySqlDb{}, err
	}
	// Ping db
	if err = db.Ping(); err != nil {
		return &MySqlDb{}, err
	} else {
		logger.Info("Database connection established")
	}
	return &MySqlDb{DB: db, logger: logger}, nil
}

// Close the database connection
func (m *MySqlDb) Close() error {
	if err := m.DB.Close(); err != nil {
		m.logger.Error("Error closing database connection: ", err)
		return err
	}
	m.logger.Info("Database connection closed")
	return nil
}

// Insert a new ledger entry into the database
func (m *MySqlDb) Insert(ctx context.Context, orderID, customerEmailAddress string, amount int64, operation, transactionDate string) error {
	stmt, err := m.DB.PrepareContext(ctx, insertQuery)
	if err != nil {
		return err
	}
	result, err := stmt.ExecContext(ctx, orderID, customerEmailAddress, amount, operation, transactionDate)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		m.logger.Warnf("Ledger entry insert failed: no entry was added for order ID %s", orderID)
		return sql.ErrNoRows
	}
	m.logger.Info("Ledger entry inserted successfully for order ID: ", orderID)
	return nil
}
