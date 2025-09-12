package db

import (
	"context"
	"database/sql"
	"log"

	"github.com/XSAM/otelsql"
	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
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
	db *sql.DB
}

func NewMysqlDb(dbName string, dsn string, trace *trace.TracerProvider) (*MySqlDb, error) { // Instrumented DB connection
	db, err := otelsql.Open(dbName, dsn, otelsql.WithTracerProvider(trace))
	if err != nil {
		return &MySqlDb{}, err
	}

	// Ping db
	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	} else {
		log.Println("Database connection established")
	}

	return &MySqlDb{db: db}, nil
}

func (i *MySqlDb) Close() error {
	if err := i.db.Close(); err != nil {
		log.Printf("Error closing database connection: %v", err)
		return err
	}
	log.Println("Database connection closed")
	return nil
}

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
	return passwordHash, nil
}

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
	return true, nil
}

func (i *MySqlDb) RegisterUser(ctx context.Context, user *pb.UserRegistrationForm) error {
	stmt, err := i.db.PrepareContext(ctx, insertUserQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, user.GetFirstName(), user.GetLastName(), user.GetUserEmail(), user.GetPasswordHash())
	if err != nil {
		return err
	}
	return nil
}
