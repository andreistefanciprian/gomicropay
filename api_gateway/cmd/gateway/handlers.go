package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	authpb "github.com/andreistefanciprian/gomicropay/api_gateway/auth/proto"
	"github.com/andreistefanciprian/gomicropay/api_gateway/internal/validator"
	mmpb "github.com/andreistefanciprian/gomicropay/api_gateway/money_movement/proto"
	"github.com/sirupsen/logrus"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HTTPUserHandler defines the main user-related HTTP endpoints.
type HTTPUserHandler interface {
	RegisterUser(w http.ResponseWriter, r *http.Request)
	LoginUser(w http.ResponseWriter, r *http.Request)
	CreateAccount(w http.ResponseWriter, r *http.Request)
	CheckBalance(w http.ResponseWriter, r *http.Request)
	CustomerPaymentAuthorize(w http.ResponseWriter, r *http.Request)
	CustomerPaymentCapture(w http.ResponseWriter, r *http.Request)
}

// Application holds dependencies for HTTP handlers.
type Application struct {
	mmClient   mmpb.MoneyMovementServiceClient // Money movement gRPC client
	authClient authpb.AuthServiceClient        // Auth gRPC client
	tracer     trace.Tracer                    // OpenTelemetry tracer
	logger     *logrus.Logger                  // Structured logger
}

// NewApplication constructs an Application with all dependencies.
func NewApplication(mmClient mmpb.MoneyMovementServiceClient, authClient authpb.AuthServiceClient, tracer trace.Tracer, logger *logrus.Logger) *Application {
	return &Application{
		mmClient:   mmClient,
		authClient: authClient,
		tracer:     tracer,
		logger:     logger,
	}
}

// RegisterUser handles user registration requests.
func (a *Application) RegisterUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "RegisterUser")
	defer span.End()

	a.logger.Info("RegisterUser handler called")

	// Parse request body
	var req struct {
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
		Email     string `json:"email"`
		Password  string `json:"password"`
	}
	span.SetAttributes(attribute.String("email", req.Email))

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		a.logger.Errorf("Failed to decode JSON body: %v", err)
		http.Error(w, "Failed to decode JSON body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.FirstName == "" || req.LastName == "" || req.Email == "" || req.Password == "" {
		a.logger.Error("One or more required fields are blank")
		http.Error(w, "All fields are required", http.StatusBadRequest)
		return
	}

	// Validate password length
	ok := validator.MinChars(req.Password, 8)
	if !ok {
		a.logger.Error("Password does not meet minimum length requirement")
		http.Error(w, "Password must be at least 8 characters long", http.StatusBadRequest)
		return
	}

	// Hash password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
	if err != nil {
		a.logger.Errorf("Failed to hash password: %v", err)
		http.Error(w, "Failed to hash password", http.StatusInternalServerError)
		return
	}

	// Check if user exists
	userExistsResp, err := a.authClient.CheckUserExists(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		a.logger.Errorf("Failed to check if user exists: %v", err)
		http.Error(w, "Failed to check if user exists", http.StatusInternalServerError)
		return
	}
	if userExistsResp.IsUser {
		a.logger.Errorf("User already exists: %s", req.Email)
		http.Error(w, "User already exists", http.StatusConflict)
		return
	}

	// Validate email format
	ok = validator.Matches(req.Email, validator.EmailRX)
	if !ok {
		a.logger.Errorf("Invalid email format: %s", req.Email)
		http.Error(w, "Invalid email format", http.StatusBadRequest)
		return
	}

	// Prepare registration data
	userRegistrationData := &authpb.UserRegistrationForm{
		FirstName:    req.FirstName,
		LastName:     req.LastName,
		UserEmail:    req.Email,
		PasswordHash: string(hashedPassword),
	}

	a.logger.Debugf("User registration data: %+v", userRegistrationData)

	// Register user via gRPC
	response, err := a.authClient.RegisterUser(ctx, userRegistrationData)
	if err != nil {
		span.AddEvent("User registration failed", trace.WithAttributes())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	span.AddEvent("User registered successfully", trace.WithAttributes())
	responseJSON, err := json.Marshal(response)
	if err != nil {
		a.logger.Errorf("Failed to marshal response: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	a.logger.Infof("RegisterUser succeeded: %v", response)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(responseJSON)
	if err != nil {
		a.logger.Errorf("Failed to write response: %v", err)
	}
}

// LoginUser handles user login and JWT token generation.
func (a *Application) LoginUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "LoginUser")
	defer span.End()

	a.logger.Infof("LoginUser handler called")

	// Parse request body
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		a.logger.Errorf("Failed to decode JSON body: %v", err)
		http.Error(w, "Failed to decode JSON body", http.StatusBadRequest)
		return
	}

	span.SetAttributes(attribute.String("email", req.Email))

	// Validate required fields
	if req.Email == "" || req.Password == "" {
		a.logger.Error("One or more required fields are blank")
		http.Error(w, "All fields are required", http.StatusBadRequest)
		return
	}

	// Check if user exists
	userExistsResp, err := a.authClient.CheckUserExists(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		a.logger.Errorf("Failed to check if user exists: %v", err)
		http.Error(w, "Failed to check if user exists", http.StatusInternalServerError)
		return
	}
	if !userExistsResp.IsUser {
		a.logger.Errorf("User is not registered: %s", req.Email)
		http.Error(w, "User is not registered", http.StatusConflict)
		return
	}

	// Retrieve password hash from auth service
	hashedPassword, err := a.authClient.RetrieveHashedPassword(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		a.logger.Errorf("Failed to retrieve hashed password: %v", err)
		http.Error(w, "Failed to retrieve hashed password", http.StatusInternalServerError)
		return
	}

	// Compare password
	if err := bcrypt.CompareHashAndPassword([]byte(hashedPassword.HashedPassword), []byte(req.Password)); err != nil {
		a.logger.Errorf("Invalid password for user %s: %v", req.Email, err)
		http.Error(w, "Invalid password", http.StatusUnauthorized)
		return
	}

	// Generate JWT token
	token, err := a.authClient.GenerateToken(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		a.logger.Errorf("GenerateToken failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}
	a.logger.Debugf("Token generated for user %s: %s", req.Email, token.Jwt)
	a.logger.Infof("LoginUser succeeded: %s", req.Email)
	_, err = w.Write([]byte(token.Jwt))
	if err != nil {
		log.Println("Failed to write token:", err)
	}
}

// CheckBalance returns the user's wallet balance.
func (a *Application) CheckBalance(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "CheckBalance")
	defer span.End()

	a.logger.Infof("CheckBalance called")

	// Validate JWT token
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logger.Errorf("Authorization failed: %v", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// Parse request body
	type checkBalancePayload struct {
		EmailAddress string `json:"email_address"`
	}

	var payload checkBalancePayload
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.logger.Errorf("Failed to read request body: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logger.Debugf("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		a.logger.Errorf("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	span.SetAttributes(attribute.String("email", payload.EmailAddress))

	a.logger.Debugf("checkBalance payload: %+v", payload)
	// Call money movement service
	authorizedResponse, err := a.mmClient.CheckBalance(ctx, &mmpb.CheckBalancePayload{
		EmailAddress: payload.EmailAddress,
	})
	if err != nil {
		a.logger.Errorf("Check balance failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}

	a.logger.Debugf("checkBalance response: %+v", authorizedResponse)
	type response struct {
		Cents int64 `json:"cents"`
	}

	resp := response{
		Cents: authorizedResponse.BalanceCents,
	}
	responseJSON, err := json.Marshal(resp)
	if err != nil {
		a.logger.Errorf("Failed to marshal response: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	a.logger.Infof("CheckBalance succeeded")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(responseJSON)
	if err != nil {
		a.logger.Errorf("Failed to write response: %v", err)
	}
}

// CustomerPaymentAuthorize handles payment authorization between customer and merchant.
func (a *Application) CustomerPaymentAuthorize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "CustomerPaymentAuthorize")
	defer span.End()

	a.logger.Infof("CustomerPaymentAuthorize called")

	// Validate JWT token
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logger.Errorf("Authorization failed: %v", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// Parse request body
	type authorizePayload struct {
		CustomerEmailAddress string `json:"customer_email_address"`
		MerchantEmailAddress string `json:"merchant_email_address"`
		Cents                int64  `json:"cents"`
		Currency             string `json:"currency"`
	}

	var payload authorizePayload
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.logger.Errorf("Failed to read request body: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logger.Debugf("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		a.logger.Errorf("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	span.SetAttributes(
		attribute.String("customer_email", payload.CustomerEmailAddress),
		attribute.String("merchant_email", payload.MerchantEmailAddress),
		attribute.String("cents", strconv.Itoa(int(payload.Cents))),
		attribute.String("currency", payload.Currency),
	)

	a.logger.Debugf("Authorize payload: %+v", payload)
	// Call money movement service
	authorizedResponse, err := a.mmClient.Authorize(ctx, &mmpb.AuthorizePayload{
		CustomerEmailAddress: payload.CustomerEmailAddress,
		MerchantEmailAddress: payload.MerchantEmailAddress,
		Cents:                payload.Cents,
		Currency:             payload.Currency,
	})
	if err != nil {
		a.logger.Errorf("Money movement authorization failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}

	type response struct {
		Pid string `json:"pid"`
	}

	resp := response{
		Pid: authorizedResponse.Pid,
	}
	responseJSON, err := json.Marshal(resp)
	if err != nil {
		a.logger.Errorf("Failed to marshal response: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	a.logger.Infof("CustomerPaymentAuthorize succeeded")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(responseJSON)
	if err != nil {
		a.logger.Errorf("Failed to write response: %v", err)
	}
}

// CustomerPaymentCapture finalizes a previously authorized payment.
func (a *Application) CustomerPaymentCapture(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "CustomerPaymentCapture")
	defer span.End()

	a.logger.Infof("CustomerPaymentCapture handler called")

	// Validate JWT token
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logger.Errorf("Authorization failed: %v", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// Parse request body
	type capturePayload struct {
		Pid string `json:"pid"`
	}

	var payload capturePayload
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.logger.Errorf("Failed to read request body: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logger.Debugf("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		a.logger.Errorf("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	span.SetAttributes(attribute.String("pid", payload.Pid))

	// Call money movement service
	a.logger.Debugf("Capture payload: %+v", payload)
	_, err = a.mmClient.Capture(ctx, &mmpb.CapturePayload{Pid: payload.Pid})
	if err != nil {
		a.logger.Errorf("Money movement capture failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}
	a.logger.Infof("CustomerPaymentCapture succeeded")
	w.WriteHeader(http.StatusOK)
}

// CreateAccount creates a new wallet for the user.
func (a *Application) CreateAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "CreateAccount")
	defer span.End()

	a.logger.Info("CreateAccount handler called")

	// Validate JWT token
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logger.Error("Authorization failed: ", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// Parse request body
	type createAccountPayload struct {
		EmailAddress           string `json:"email_address"`
		WalletType             string `json:"wallet_type"`
		InitialBalanceCents    int64  `json:"initial_balance_cents"`
		InitialBalanceCurrency string `json:"initial_balance_currency"`
	}

	var payload createAccountPayload
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.logger.Errorf("Failed to read request body: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logger.Debugf("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		a.logger.Errorf("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Validate WalletType
	var walletTypeEnum mmpb.WalletType
	switch strings.ToUpper(payload.WalletType) {
	case "CUSTOMER":
		walletTypeEnum = mmpb.WalletType_CUSTOMER
	case "MERCHANT":
		walletTypeEnum = mmpb.WalletType_MERCHANT
	default:
		a.logger.Error("Invalid WalletType: ", payload.WalletType)
		http.Error(w, "wallet_type must be CUSTOMER or MERCHANT", http.StatusBadRequest)
		return
	}

	span.SetAttributes(
		attribute.String("email", payload.EmailAddress),
		attribute.String("wallet_type", payload.WalletType),
		attribute.String("initial_balance_cents", strconv.Itoa(int(payload.InitialBalanceCents))),
		attribute.String("initial_balance_currency", payload.InitialBalanceCurrency),
	)

	// Call money movement service
	a.logger.Debug("Create Account payload: ", payload)
	_, err = a.mmClient.CreateAccount(ctx, &mmpb.CreateAccountPayload{
		EmailAddress:           payload.EmailAddress,
		WalletType:             walletTypeEnum,
		InitialBalanceCents:    payload.InitialBalanceCents,
		InitialBalanceCurrency: payload.InitialBalanceCurrency,
	})
	if err != nil {
		a.logger.Error("Create Account failed: ", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			a.logger.Error("Failed to write error response: ", writeErr)
		}
		return
	}
	a.logger.Info("CreateAccount succeeded")
	w.WriteHeader(http.StatusOK)
}

// checkAuthHeader validates the Authorization header and token.
func (a *Application) checkAuthHeader(context context.Context, r *http.Request) error {
	authHeader := r.Header.Get("Authorization")
	a.logger.Debugf("Authorization header: %s", authHeader)
	if authHeader == "" {
		a.logger.Error("Missing Authorization header")
		return status.Error(codes.Unauthenticated, "missing Authorization header")
	}
	if !strings.HasPrefix(authHeader, "Bearer ") {
		a.logger.Error("Authorization header does not start with 'Bearer '")
		return status.Error(codes.Unauthenticated, "invalid Authorization header")
	}
	token := strings.TrimPrefix(authHeader, "Bearer ")
	a.logger.Debugf("Extracted token: %s", token)

	if token == "" {
		a.logger.Error("Token is empty after trimming 'Bearer ' prefix")
		return status.Error(codes.Unauthenticated, "empty token")
	}

	// Validate the token
	_, err := a.validateToken(context, token)
	if err != nil {
		a.logger.Errorf("Token validation failed: %v", err)
		return status.Error(codes.Unauthenticated, "invalid token")
	}

	return nil
}

// validateToken checks the JWT token with the auth service.
func (a *Application) validateToken(context context.Context, token string) (*authpb.UserEmailAddress, error) {
	ctx, span := a.tracer.Start(context, "validateToken")
	defer span.End()

	email, err := a.authClient.VerifyToken(ctx, &authpb.Token{Jwt: token})
	if err != nil {
		a.logger.Errorf("Token validation failed: %v", err)
		return email, status.Error(codes.Unauthenticated, "invalid token")
	}

	span.SetAttributes(attribute.String("email", email.UserEmail))

	a.logger.Debugf("Token is valid for user: %s", email.UserEmail)

	return email, nil
}

// withNotFoundHandler returns 404 for unknown routes.
func withNotFoundHandler(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler, pattern := mux.Handler(r)
		if pattern == "" {
			http.Error(w, "Error: Route not found", http.StatusNotFound)
			fmt.Println("Error: Route not found for", r.Method, r.URL.Path)
			return
		}
		handler.ServeHTTP(w, r)
	})
}
