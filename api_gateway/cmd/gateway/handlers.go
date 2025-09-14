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

type HTTPUserHandler interface {
	RegisterUser(w http.ResponseWriter, r *http.Request)
	LoginUser(w http.ResponseWriter, r *http.Request)
	CreateAccount(w http.ResponseWriter, r *http.Request)
	CheckBalance(w http.ResponseWriter, r *http.Request)
	CustomerPaymentAuthorize(w http.ResponseWriter, r *http.Request)
	CustomerPaymentCapture(w http.ResponseWriter, r *http.Request)
}

type Application struct {
	mmClient   mmpb.MoneyMovementServiceClient
	authClient authpb.AuthServiceClient
	tracer     trace.Tracer
	logger     *logrus.Logger
}

func NewApplication(mmClient mmpb.MoneyMovementServiceClient, authClient authpb.AuthServiceClient, tracer trace.Tracer, logger *logrus.Logger) *Application {
	return &Application{
		mmClient:   mmClient,
		authClient: authClient,
		tracer:     tracer,
		logger:     logger,
	}
}

func (a *Application) RegisterUser(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the RegisterUser operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "RegisterUser")
	defer span.End()

	a.logger.Info("RegisterUser handler called")

	var req struct {
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
		Email     string `json:"email"`
		Password  string `json:"password"`
	}
	span.SetAttributes(
		attribute.String("email", req.Email),
	)

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		a.logger.Errorf("Failed to decode JSON body: %v", err)
		http.Error(w, "Failed to decode JSON body", http.StatusBadRequest)
		return
	}

	// Verify Blank fields
	if req.FirstName == "" || req.LastName == "" || req.Email == "" || req.Password == "" {
		a.logger.Error("One or more required fields are blank")
		http.Error(w, "All fields are required", http.StatusBadRequest)
		return
	}

	// Verify password length
	ok := validator.MinChars(req.Password, 8)
	if !ok {
		a.logger.Error("Password does not meet minimum length requirement")
		http.Error(w, "Password must be at least 8 characters long", http.StatusBadRequest)
		return
	}

	// Create a bcrypt hash of the plain-text password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
	if err != nil {
		a.logger.Errorf("Failed to hash password: %v", err)
		http.Error(w, "Failed to hash password", http.StatusInternalServerError)
		return
	}

	//  Check if user exists
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

	//  Verify email format
	ok = validator.Matches(req.Email, validator.EmailRX)
	if !ok {
		a.logger.Errorf("Invalid email format: %s", req.Email)
		http.Error(w, "Invalid email format", http.StatusBadRequest)
		return
	}

	// Register user
	userRegistrationData := &authpb.UserRegistrationForm{
		FirstName:    req.FirstName,
		LastName:     req.LastName,
		UserEmail:    req.Email,
		PasswordHash: string(hashedPassword),
	}

	a.logger.Debugf("User registration data: %+v", userRegistrationData)

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

func (a *Application) LoginUser(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the LoginUser operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "LoginUser")
	defer span.End()

	a.logger.Infof("LoginUser handler called")

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

	span.SetAttributes(
		attribute.String("email", req.Email),
	)

	// Verify Blank fields
	if req.Email == "" || req.Password == "" {
		a.logger.Error("One or more required fields are blank")
		http.Error(w, "All fields are required", http.StatusBadRequest)
		return
	}

	//  Check if user exists
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

	// Get password hash from auth service
	hashedPassword, err := a.authClient.RetrieveHashedPassword(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		a.logger.Errorf("Failed to retrieve hashed password: %v", err)
		http.Error(w, "Failed to retrieve hashed password", http.StatusInternalServerError)
		return
	}

	// Check password is valid
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

func (a *Application) CheckBalance(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the CheckBalance operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "CheckBalance")
	defer span.End()

	a.logger.Infof("CheckBalance called")

	// Check user has valid JWT Token in Authorisation Header
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logger.Errorf("Authorization failed: %v", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// Call money movement service
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

	span.SetAttributes(
		attribute.String("email", payload.EmailAddress),
	)

	a.logger.Debugf("checkBalance payload: %+v", payload)
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

func (a *Application) CustomerPaymentAuthorize(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the CustomerPaymentAuthorize operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "CustomerPaymentAuthorize")
	defer span.End()

	a.logger.Infof("CustomerPaymentAuthorize called")

	// Check user has valid JWT Token in Authorisation Header
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logger.Errorf("Authorization failed: %v", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// Call money movement service
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

func (a *Application) CustomerPaymentCapture(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the CustomerPaymentCapture operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "CustomerPaymentCapture")
	defer span.End()

	a.logger.Infof("CustomerPaymentCapture handler called")

	// Check user has valid JWT Token in Authorisation Header
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logger.Errorf("Authorization failed: %v", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// Call money movement service
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

	span.SetAttributes(
		attribute.String("pid", payload.Pid),
	)

	// Capture payment
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

func (a *Application) CreateAccount(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the CreateAccount operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "CreateAccount")
	defer span.End()

	a.logger.Info("CreateAccount handler called")

	// Check user has valid JWT Token in Authorisation Header
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logger.Error("Authorization failed: ", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

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

	// Validate WalletType is either CUSTOMER or MERCHANT
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

	// Validate JWT token user is the same as payload.EmailAddress

	// Create account
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

func (a *Application) validateToken(context context.Context, token string) (*authpb.UserEmailAddress, error) {
	// Start a new span for the validateToken operation
	ctx, span := a.tracer.Start(context, "validateToken")
	defer span.End()

	email, err := a.authClient.VerifyToken(ctx, &authpb.Token{Jwt: token})
	if err != nil {
		a.logger.Errorf("Token validation failed: %v", err)
		return email, status.Error(codes.Unauthenticated, "invalid token")
	}

	span.SetAttributes(
		attribute.String("email", email.UserEmail),
	)

	a.logger.Debugf("Token is valid for user: %s", email.UserEmail)

	return email, nil
}

// withNotFoundHandler wraps the HTTP handler to return a 404 Not Found error
// if the requested route does not match any registered handlers
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
