package db

import (
	"context"
	"database/sql"

	"github.com/XSAM/otelsql"
	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/sdk/trace"
)

const (
	selectPasswordHashQuery = "SELECT password_hash FROM registered_users WHERE email = ?"
	checkUserExistsQuery    = "SELECT COUNT(*) FROM registered_users WHERE email = ?"
	insertUserQuery         = "INSERT INTO registered_users (first_name, last_name, email, password_hash) VALUES (?, ?, ?, ?)"
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
		i.logger.Warn("User registration failed: no user was added for email ", user.GetUserEmail())
		return sql.ErrNoRows
	}
	i.logger.Info("User registered successfully: ", user.GetUserEmail())
	return nil
}
