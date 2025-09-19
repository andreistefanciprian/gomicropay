package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/XSAM/otelsql"
	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/sdk/trace"
)

const (
	selectPasswordHashQuery = "SELECT password_hash FROM registered_users WHERE email = ?"
	checkUserExistsQuery    = "SELECT COUNT(*) FROM registered_users WHERE email = ?"
	insertUserQuery         = "INSERT INTO registered_users (first_name, last_name, email, password_hash) VALUES (?, ?, ?, ?)"
	dbDriver                = "mysql"
)

type AuthRepository interface {
	RetrieveHashedPassword(ctx context.Context, email string) (string, error)
	CheckUserExists(ctx context.Context, email string) (bool, error)
	RegisterUser(ctx context.Context, user *pb.UserRegistrationForm) error
	Close() error
}

type MySqlDb struct {
	db     *sql.DB
	logger *logrus.Logger
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
func NewMysqlDb(tracerProvider *trace.TracerProvider, logger *logrus.Logger) (*MySqlDb, error) {
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
	return &MySqlDb{db: db, logger: logger}, nil
}

// Close the database connection
func (i *MySqlDb) Close() error {
	if err := i.db.Close(); err != nil {
		i.logger.Error("Error closing database connection: ", err)
		return err
	}
	i.logger.Info("Database connection closed")
	return nil
}

// RetrieveHashedPassword fetches the password hash for a given email
func (i *MySqlDb) RetrieveHashedPassword(ctx context.Context, email string) (string, error) {
	var passwordHash string
	stmt, err := i.db.PrepareContext(ctx, selectPasswordHashQuery)
	if err != nil {
		return "", err
	}
	defer stmt.Close()
	err = stmt.QueryRowContext(ctx, email).Scan(&passwordHash)
	if err != nil {
		return "", err
	}
	i.logger.Info("Retrieved password hash for email: ", email)
	return passwordHash, nil
}

// CheckUserExists checks if a user exists for a given email
func (i *MySqlDb) CheckUserExists(ctx context.Context, email string) (bool, error) {
	stmt, err := i.db.PrepareContext(ctx, checkUserExistsQuery)
	if err != nil {
		return false, err
	}
	defer stmt.Close()
	var count int
	err = stmt.QueryRowContext(ctx, email).Scan(&count)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	i.logger.Infof("User exists: %s", email)
	return true, nil
}

// RegisterUser registers a new user in the database
func (i *MySqlDb) RegisterUser(ctx context.Context, user *pb.UserRegistrationForm) error {
	stmt, err := i.db.PrepareContext(ctx, insertUserQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	result, err := stmt.ExecContext(ctx, user.GetFirstName(), user.GetLastName(), user.GetUserEmail(), user.GetPasswordHash())
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		i.logger.Warnf("User registration failed: no user was added for email %s", user.GetUserEmail())
		return sql.ErrNoRows
	}
	i.logger.Infof("User registered successfully: %s", user.GetUserEmail())
	return nil
}
