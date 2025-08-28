package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/andreistefanciprian/gomicropay/auth/internal/implementation/auth"
	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
)

const (
	dbDriver = "mysql"
)

var db *sql.DB

func main() {

	var err error

	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	dbName := os.Getenv("MYSQL_DB")
	dbHost := os.Getenv("MYSQL_HOST")
	dbPort := os.Getenv("MYSQL_PORT")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPassword, dbHost, dbPort, dbName)

	db, err = sql.Open(dbDriver, dsn)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err = db.Close(); err != nil {
			log.Printf("Error closing database connection: %v", err)
		}
		log.Println("Database connection closed")
	}()

	// Ping db
	if err = db.Ping(); err != nil {

		log.Fatalf("Failed to connect to the database: %v", err)
	} else {
		log.Println("Database connection established")
	}

	// gRPC server setup
	grpcServer := grpc.NewServer()
	authServerImplementation := auth.NewAuthImplementation(db)
	pb.RegisterAuthServiceServer(grpcServer, authServerImplementation)

	// listen and serve
	listener, err := net.Listen("tcp", ":9000")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
	log.Printf("gRPC server listening at %v", listener.Addr())
}
