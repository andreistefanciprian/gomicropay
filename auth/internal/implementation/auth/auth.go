package auth

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"

	"github.com/andreistefanciprian/gomicropay/auth/internal/db"
	pb "github.com/andreistefanciprian/gomicropay/auth/proto"
	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Implementation struct {
	db db.AuthRepository
	pb.UnimplementedAuthServiceServer
	tracer trace.Tracer
	logger *logrus.Logger
}

// NewAuthImplementation constructs a new Implementation for the Auth service.
func NewAuthImplementation(db db.AuthRepository, tracer trace.Tracer, logger *logrus.Logger) *Implementation {
	return &Implementation{
		db:     db,
		tracer: tracer,
		logger: logger,
	}
}

// RetrieveHashedPassword returns the hashed password for a given user email.
func (i *Implementation) RetrieveHashedPassword(ctx context.Context, userEmail *pb.UserEmailAddress) (*pb.HashedPassword, error) {
	ctx, span := i.tracer.Start(ctx, "RetrieveHashedPassword")
	defer span.End()

	email := userEmail.GetUserEmail()
	span.SetAttributes(
		attribute.String("user_email", email),
	)
	i.logger.Debugf("RetrieveHashedPassword called for email: %s", email)
	passwordHash, err := i.db.RetrieveHashedPassword(ctx, email)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			i.logger.Warnf("RetrieveHashedPassword: user not found for email: %s", email)
			return nil, status.Error(codes.NotFound, "user not found")
		}
		i.logger.Errorf("RetrieveHashedPassword failed: query error: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	span.AddEvent("RetrieveHashedPassword succeeded", trace.WithAttributes())
	i.logger.Info("RetrieveHashedPassword succeeded for email: %s", email)
	return &pb.HashedPassword{HashedPassword: passwordHash}, nil
}

// CheckUserExists checks if a user exists for the given email.
func (i *Implementation) CheckUserExists(ctx context.Context, in *pb.UserEmailAddress) (*pb.UserExistsResponse, error) {
	ctx, span := i.tracer.Start(ctx, "CheckUserExists")
	defer span.End()

	span.SetAttributes(
		attribute.String("user_email", in.GetUserEmail()),
	)
	i.logger.Debugf("CheckUserExists called for email: %s", in.GetUserEmail())

	exists, err := i.db.CheckUserExists(ctx, in.GetUserEmail())
	if err != nil {
		span.AddEvent("CheckUserExists failed: db op", trace.WithAttributes())
		i.logger.Errorf("CheckUserExists failed: query error: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	i.logger.Infof("CheckUserExists: %v for email: %s", exists, in.GetUserEmail())
	return &pb.UserExistsResponse{IsUser: exists}, nil
}

// RegisterUser registers a new user in the system.
func (i *Implementation) RegisterUser(ctx context.Context, user *pb.UserRegistrationForm) (*pb.UserRegistrationResponse, error) {
	ctx, span := i.tracer.Start(ctx, "RegisterUser")
	defer span.End()

	span.SetAttributes(
		attribute.String("user_email", user.GetUserEmail()),
	)
	i.logger.Debugf("User details: FirstName=%s, LastName=%s, Email=%s", user.GetFirstName(), user.GetLastName(), user.GetUserEmail())

	err := i.db.RegisterUser(ctx, user)
	if err != nil {
		span.AddEvent("RegisterUser failed: exec error", trace.WithAttributes())
		i.logger.Errorf("RegisterUser failed: exec error: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	span.AddEvent("RegisterUser succeeded", trace.WithAttributes())
	i.logger.Infof("RegisterUser succeeded for email: %s", user.GetUserEmail())
	return &pb.UserRegistrationResponse{IsRegistered: true}, nil
}

// GenerateToken creates a JWT token for the given user email.
func (i *Implementation) GenerateToken(ctx context.Context, email *pb.UserEmailAddress) (*pb.Token, error) {
	// Start a new span for the GenerateToken operation
	_, span := i.tracer.Start(ctx, "GenerateToken")
	defer span.End()

	span.SetAttributes(
		attribute.String("user_email", email.GetUserEmail()),
	)
	i.logger.Debugf("GenerateToken called for email: %s", email.GetUserEmail())
	jwToken, err := createJWT(email.GetUserEmail())
	if err != nil {
		i.logger.Errorf("GenerateToken failed: JWT creation error: %v", err)
		return nil, err
	}
	i.logger.Infof("GenerateToken succeeded for email: %s", email.GetUserEmail())
	return &pb.Token{Jwt: jwToken}, nil
}

// VerifyToken validates a JWT token and returns the associated user email.
func (i *Implementation) VerifyToken(ctx context.Context, token *pb.Token) (*pb.UserEmailAddress, error) {
	// Start a new span for the VerifyToken operation
	_, span := i.tracer.Start(ctx, "VerifyToken")
	defer span.End()
	i.logger.Debugf("VerifyToken called for token: %s", token.Jwt)
	key := []byte(os.Getenv("SIGNING_KEY"))
	emailAddress, err := validateJWT(token.Jwt, key)
	if err != nil {
		i.logger.Errorf("VerifyToken failed: JWT validation error: %v", err)
		return nil, err
	}
	i.logger.Infof("VerifyToken succeeded for email: %s", emailAddress)
	return &pb.UserEmailAddress{UserEmail: emailAddress}, nil
}

// validateJWT parses and validates a JWT token, returning the subject (email) if valid.
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

// createJWT generates a signed JWT token for the given email address.
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
