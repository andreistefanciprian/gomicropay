package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"

	mm "github.com/andreistefanciprian/gomicropay/money_movement/internal/implementation"
	pb "github.com/andreistefanciprian/gomicropay/money_movement/proto"
	"google.golang.org/grpc"
)

const (
	dbDriver   = "mysql"
	dbUser     = "root"
	dbPassword = "Admin123"
	dbName     = "money_movement"
)

var db *sql.DB

func main() {

	var err error
	dsn := fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s", dbUser, dbPassword, dbName)
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
	pb.RegisterMoneyMovementServiceServer(grpcServer, mm.NewMoneyMovementImplementation(db))
	// listen and serve
	listener, err := net.Listen("tcp", ":7000")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
	log.Printf("gRPC server listening at %v", listener.Addr())
}
