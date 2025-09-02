package auth

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"time"

	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
	jwt "github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var logLevel = getLogLevel()

func getLogLevel() string {
	lvl := os.Getenv("LOG_LEVEL")
	if lvl == "" {
		return "INFO"
	}
	return lvl
}

func logInfo(format string, v ...interface{}) {
	if logLevel == "INFO" || logLevel == "DEBUG" {
		log.Printf("INFO: "+format, v...)
	}
}

func logDebug(format string, v ...interface{}) {
	if logLevel == "DEBUG" {
		log.Printf("DEBUG: "+format, v...)
	}
}

const (
	selectPasswordHashQuery = "SELECT password_hash FROM registered_users WHERE email = ?"
	checkUserExistsQuery    = "SELECT COUNT(*) FROM registered_users WHERE email = ?"
	insertUserQuery         = "INSERT INTO registered_users (first_name, last_name, email, password_hash) VALUES (?, ?, ?, ?)"
)

type Implementation struct {
	db *sql.DB
	pb.UnimplementedAuthServiceServer
}

func NewAuthImplementation(db *sql.DB) *Implementation {
	return &Implementation{
		db: db,
	}
}

func (i *Implementation) RetrieveHashedPassword(ctx context.Context, userEmail *pb.UserEmailAddress) (*pb.HashedPassword, error) {
	type user struct {
		passwordHash string
	}

	var u user

	stmt, err := i.db.Prepare(selectPasswordHashQuery)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	err = stmt.QueryRow(userEmail.GetUserEmail()).Scan(&u.passwordHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Error(codes.Unauthenticated, "invalid email address")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.HashedPassword{HashedPassword: u.passwordHash}, nil
}

func (i *Implementation) CheckUserExists(ctx context.Context, in *pb.UserEmailAddress) (*pb.UserExistsResponse, error) {
	var count int
	stmt, err := i.db.Prepare(checkUserExistsQuery)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()
	err = stmt.QueryRow(in.GetUserEmail()).Scan(&count)
	if err != nil {
		log.Println(err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if count == 0 {
		return &pb.UserExistsResponse{IsUser: false}, nil
	}

	return &pb.UserExistsResponse{IsUser: true}, nil
}

func (i *Implementation) RegisterUser(ctx context.Context, in *pb.UserRegistrationForm) (*pb.UserRegistrationResponse, error) {
	logDebug("User details: FirstName=%s, LastName=%s, Email=%s", in.GetFirstName(), in.GetLastName(), in.GetUserEmail())
	stmt, err := i.db.Prepare(insertUserQuery)
	if err != nil {
		logInfo("RegisterUser failed: prepare error: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(in.GetFirstName(), in.GetLastName(), in.GetUserEmail(), in.GetPasswordHash())
	if err != nil {
		logInfo("RegisterUser failed: exec error: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	logInfo("RegisterUser succeeded for email: %s", in.GetUserEmail())
	return &pb.UserRegistrationResponse{IsRegistered: true}, nil
}

func (i *Implementation) GenerateToken(ctx context.Context, email *pb.UserEmailAddress) (*pb.Token, error) {
	jwToken, err := createJWT(email.GetUserEmail())
	if err != nil {
		return nil, err
	}
	return &pb.Token{Jwt: jwToken}, nil
}

func (i *Implementation) VerifyToken(ctx context.Context, token *pb.Token) (*pb.UserEmailAddress, error) {
	key := []byte(os.Getenv("SIGNING_KEY"))
	emailAddress, err := validateJWT(token.Jwt, key)
	if err != nil {
		return nil, err
	}
	return &pb.UserEmailAddress{UserEmail: emailAddress}, nil
}

func validateJWT(tokenString string, signingKey []byte) (string, error) {
	// parse token
	type MyClaims struct {
		jwt.RegisteredClaims
	}
	parsedToken, err := jwt.ParseWithClaims(tokenString, &MyClaims{}, func(token *jwt.Token) (interface{}, error) {
		return signingKey, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return "", status.Error(codes.Unauthenticated, "token expired")
		} else {
			return "", status.Error(codes.Unauthenticated, "unauthenticated")
		}
	}
	claims, ok := parsedToken.Claims.(*MyClaims)
	if !ok {
		return "", status.Error(codes.Internal, "invalid token claims")
	}
	return claims.Subject, nil
}

func createJWT(emailAddress string) (string, error) {
	key := []byte(os.Getenv("SIGNING_KEY"))
	claims := jwt.MapClaims{
		"iss": "auth-service",
		"sub": emailAddress,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour * 24).Unix(),
		// You can add more claims here, like expiration, etc.
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signedToken, err := token.SignedString(key)
	if err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	return signedToken, nil
}
