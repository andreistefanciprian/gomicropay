package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/XSAM/otelsql"
	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
	"github.com/go-sql-driver/mysql"
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

// getConfig retrieves database configuration from environment variables and returns a mysql.Config
func loadCfgFromEnv() (*mysql.Config, error) {
	user := os.Getenv("MYSQL_USER")
	pass := os.Getenv("MYSQL_PASSWORD")
	db := os.Getenv("MYSQL_DB")
	host := os.Getenv("MYSQL_HOST")
	port := os.Getenv("MYSQL_PORT")
	if user == "" || pass == "" || host == "" || port == "" || db == "" {
		return nil, errors.New("database configuration environment variables are not fully set")
	}
	cfg := &mysql.Config{
		User:                 user,
		Passwd:               pass,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%s", host, port),
		DBName:               db,
		AllowNativePasswords: true,
		ParseTime:            true,
	}
	return cfg, nil
}

// NewMysqlDb creates a new MySqlDb instance with an instrumented DB connection
func NewMysqlDb(tracerProvider *trace.TracerProvider, logger *logrus.Logger) (*MySqlDb, error) {
	// Get database configuration
	config, err := loadCfgFromEnv()
	if err != nil {
		return nil, err
	}

	// Use FormatDSN to generate the DSN string
	dsn := config.FormatDSN()
	db, err := otelsql.Open(dbDriver, dsn, otelsql.WithTracerProvider(tracerProvider))
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
