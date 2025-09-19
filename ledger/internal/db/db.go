package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/XSAM/otelsql"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

const (
	insertQuery = "INSERT INTO ledger (order_id, customer_email_address, amount, operation, transaction_date) VALUES (?, ?, ?, ?, ?)"
	dbDriver    = "mysql"
)

type LedgerRepository interface {
	Insert(ctx context.Context, orderID, customerEmailAddress string, amount int64, operation, transactionDate string) error
}

type MySqlDb struct {
	DB     *sql.DB
	logger *logrus.Logger
	tracer trace.Tracer
}

type cfg struct {
	username string
	password string
	host     string
	port     string
	database string
}

func (c *cfg) dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", c.username, c.password, c.host, c.port, c.database)
}

// getConfig retrieves database configuration from environment variables
func loadCfgFromEnv() (*cfg, error) {
	username := os.Getenv("MYSQL_USER")
	password := os.Getenv("MYSQL_PASSWORD")
	database := os.Getenv("MYSQL_DB")
	host := os.Getenv("MYSQL_HOST")
	port := os.Getenv("MYSQL_PORT")
	c := &cfg{
		username: username,
		password: password,
		host:     host,
		port:     port,
		database: database,
	}
	if c.username == "" || c.password == "" || c.host == "" || c.port == "" || c.database == "" {
		return nil, errors.New("database configuration environment variables are not fully set")
	}
	return c, nil
}

// NewMysqlDb creates a new MySqlDb instance with an instrumented DB connection
func NewMysqlDb(tracerProvider trace.TracerProvider, logger *logrus.Logger) (*MySqlDb, error) {
	// Get database configuration
	config, err := loadCfgFromEnv()
	if err != nil {
		return nil, err
	}

	// Open the database connection
	dbURL := config.dsn()
	db, err := otelsql.Open(dbDriver, dbURL, otelsql.WithTracerProvider(tracerProvider))
	if err != nil {
		return nil, err
	}
	// Ping db
	if err = db.Ping(); err != nil {
		return nil, err
	} else {
		logger.Info("Database connection established")
	}
	tracer := tracerProvider.Tracer("db")
	return &MySqlDb{DB: db, logger: logger, tracer: tracer}, nil
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
	ctx, span := m.tracer.Start(ctx, "Insert")
	defer span.End()

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
	m.logger.Infof("Ledger entry inserted successfully for order ID: %s", orderID)
	return nil
}
