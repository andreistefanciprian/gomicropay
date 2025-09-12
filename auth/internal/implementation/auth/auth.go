package auth

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"time"

	"github.com/andreistefanciprian/gomicropay/auth/internal/db"
	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
	jwt "github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

type Implementation struct {
	db db.AuthRepository
	pb.UnimplementedAuthServiceServer
	tracer trace.Tracer
}

func NewAuthImplementation(db db.AuthRepository, tracer trace.Tracer) *Implementation {
	return &Implementation{
		db:     db,
		tracer: tracer,
	}
}

func (i *Implementation) RetrieveHashedPassword(ctx context.Context, userEmail *pb.UserEmailAddress) (*pb.HashedPassword, error) {
	ctx, span := i.tracer.Start(ctx, "RetrieveHashedPassword")
	defer span.End()

	email := userEmail.GetUserEmail()
	span.SetAttributes(
		attribute.String("user_email", email),
	)

	logDebug("RetrieveHashedPassword called for email: %s", email)
	passwordHash, err := i.db.RetrieveHashedPassword(ctx, email)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			logInfo("RetrieveHashedPassword: user not found for email: %s", email)
			return nil, status.Error(codes.NotFound, "user not found")
		}
		logInfo("RetrieveHashedPassword failed: query error: %v", err)
		log.Println(err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	span.AddEvent("RetrieveHashedPassword succeeded", trace.WithAttributes())
	logInfo("RetrieveHashedPassword succeeded for email: %s", email)
	return &pb.HashedPassword{HashedPassword: passwordHash}, nil
}

func (i *Implementation) CheckUserExists(ctx context.Context, in *pb.UserEmailAddress) (*pb.UserExistsResponse, error) {
	ctx, span := i.tracer.Start(ctx, "CheckUserExists")
	defer span.End()

	span.SetAttributes(
		attribute.String("user_email", in.GetUserEmail()),
	)

	logDebug("CheckUserExists called for email: %s", in.GetUserEmail())

	exists, err := i.db.CheckUserExists(ctx, in.GetUserEmail())
	if err != nil {
		span.AddEvent("CheckUserExists failed: db op", trace.WithAttributes())
		logInfo("CheckUserExists failed: query error: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	logInfo("CheckUserExists: %v for email: %s", exists, in.GetUserEmail())
	return &pb.UserExistsResponse{IsUser: exists}, nil
}

func (i *Implementation) RegisterUser(ctx context.Context, user *pb.UserRegistrationForm) (*pb.UserRegistrationResponse, error) {
	ctx, span := i.tracer.Start(ctx, "RegisterUser")
	defer span.End()

	span.SetAttributes(
		attribute.String("user_email", user.GetUserEmail()),
	)

	logDebug("User details: FirstName=%s, LastName=%s, Email=%s", user.GetFirstName(), user.GetLastName(), user.GetUserEmail())

	err := i.db.RegisterUser(ctx, user)
	if err != nil {
		span.AddEvent("RegisterUser failed: exec error", trace.WithAttributes())
		logInfo("RegisterUser failed: exec error: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	span.AddEvent("RegisterUser succeeded", trace.WithAttributes())
	logInfo("RegisterUser succeeded for email: %s", user.GetUserEmail())
	return &pb.UserRegistrationResponse{IsRegistered: true}, nil
}

func (i *Implementation) GenerateToken(ctx context.Context, email *pb.UserEmailAddress) (*pb.Token, error) {
	// Start a new span for the GenerateToken operation
	_, span := i.tracer.Start(ctx, "GenerateToken")
	defer span.End()

	span.SetAttributes(
		attribute.String("user_email", email.GetUserEmail()),
	)

	logDebug("GenerateToken called for email: %s", email.GetUserEmail())
	jwToken, err := createJWT(email.GetUserEmail())
	if err != nil {
		logInfo("GenerateToken failed: JWT creation error: %v", err)
		return nil, err
	}
	logInfo("GenerateToken succeeded for email: %s", email.GetUserEmail())
	return &pb.Token{Jwt: jwToken}, nil
}

func (i *Implementation) VerifyToken(ctx context.Context, token *pb.Token) (*pb.UserEmailAddress, error) {
	// Start a new span for the VerifyToken operation
	_, span := i.tracer.Start(ctx, "VerifyToken")
	defer span.End()

	logDebug("VerifyToken called for token: %s", token.Jwt)
	key := []byte(os.Getenv("SIGNING_KEY"))
	emailAddress, err := validateJWT(token.Jwt, key)
	if err != nil {
		logInfo("VerifyToken failed: JWT validation error: %v", err)
		return nil, err
	}
	logInfo("VerifyToken succeeded for email: %s", emailAddress)
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
