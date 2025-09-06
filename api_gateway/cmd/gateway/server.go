package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	authpb "github.com/andreistefanciprian/gomicropay/api_gateway/auth/proto"
	"github.com/andreistefanciprian/gomicropay/api_gateway/internal/tracing"
	"github.com/andreistefanciprian/gomicropay/api_gateway/internal/validator"
	mmpb "github.com/andreistefanciprian/gomicropay/api_gateway/money_movement/proto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Application struct {
	mmClient   mmpb.MoneyMovementServiceClient
	authClient authpb.AuthServiceClient
	logLevel   string
	tracer     trace.Tracer
}

func NewApplication() *Application {
	return &Application{}
}

func (a *Application) logDebug(format string, v ...interface{}) {
	if a.logLevel == "DEBUG" {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func (a *Application) logInfo(format string, v ...interface{}) {
	if a.logLevel == "INFO" || a.logLevel == "DEBUG" {
		log.Printf("[INFO] "+format, v...)
	}
}

func main() {

	app := NewApplication()
	// Initialize tracer
	tp, err := tracing.InitTracer("api-gateway")
	if err != nil {
		log.Fatalf("failed to initialize tracer: %v", err)
	}
	defer tp.Shutdown(context.Background())
	app.tracer = tp.Tracer("api-gateway")

	// Service addresses
	authHost := os.Getenv("AUTH_HOST")
	authPort := os.Getenv("AUTH_PORT")
	moneyMovementHost := os.Getenv("MONEY_MOVEMENT_HOST")
	moneyMovementPort := os.Getenv("MONEY_MOVEMENT_PORT")
	moneyMovementAddress := fmt.Sprintf("%s:%s", moneyMovementHost, moneyMovementPort)
	authAddress := fmt.Sprintf("%s:%s", authHost, authPort)

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "INFO"
	}
	app.logLevel = logLevel
	app.logInfo("Starting API Gateway with log level: %s", logLevel)

	// auth connection
	authConn, err := grpc.NewClient(
		authAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(
			otelgrpc.NewClientHandler(
				// Optional knobs:
				otelgrpc.WithTracerProvider(tp),
				otelgrpc.WithPropagators(propagation.TraceContext{}),
			// otelgrpc.WithMessageEvents(otelgrpc.ReceivedEvents, otelgrpc.SentEvents),
			),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := authConn.Close(); err != nil {
			log.Println("Failed to close auth connection:", err)
		}
	}()

	app.authClient = authpb.NewAuthServiceClient(authConn)

	// money movement connection
	log.Printf("Connecting to Money Movement service at %s", moneyMovementAddress)
	mmConn, err := grpc.NewClient(moneyMovementAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Money Movement service at %s: %v", moneyMovementAddress, err)
	}
	defer func() {
		if err := mmConn.Close(); err != nil {
			log.Println("Failed to close money movement connection:", err)
		} else {
			log.Println("Closed money movement connection successfully.")
		}
	}()
	log.Println("Money Movement gRPC connection established.")
	app.mmClient = mmpb.NewMoneyMovementServiceClient(mmConn)

	http.HandleFunc("/register", app.register)
	http.HandleFunc("/login", app.login)
	http.HandleFunc("/create-account", app.createAccount)
	http.HandleFunc("/customer/payment/authorize", app.customerPaymentAuthorize)
	http.HandleFunc("/customer/payment/capture", app.customerPaymentCapture)
	http.HandleFunc("/checkbalance", app.checkBalance)

	log.Println("API Gateway listening on port 8080")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}

func (a *Application) register(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the register operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "register")
	defer span.End()

	a.logInfo("register handler called")

	var req struct {
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
		Email     string `json:"email"`
		Password  string `json:"password"`
	}
	span.SetAttributes(
		attribute.String("user_email", req.Email),
	)

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		a.logInfo("Failed to decode JSON body: %v", err)
		http.Error(w, "Failed to decode JSON body", http.StatusBadRequest)
		return
	}

	// Verify Blank fields
	if req.FirstName == "" || req.LastName == "" || req.Email == "" || req.Password == "" {
		a.logInfo("One or more required fields are blank")
		http.Error(w, "All fields are required", http.StatusBadRequest)
		return
	}

	// Verify password length
	ok := validator.MinChars(req.Password, 8)
	if !ok {
		a.logInfo("Password does not meet minimum length requirement")
		http.Error(w, "Password must be at least 8 characters long", http.StatusBadRequest)
		return
	}

	// Create a bcrypt hash of the plain-text password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
	if err != nil {
		a.logInfo("Failed to hash password: %v", err)
		http.Error(w, "Failed to hash password", http.StatusInternalServerError)
		return
	}

	//  Check if user exists
	userExistsResp, err := a.authClient.CheckUserExists(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		a.logInfo("Failed to check if user exists: %v", err)
		http.Error(w, "Failed to check if user exists", http.StatusInternalServerError)
		return
	}
	if userExistsResp.IsUser {
		a.logInfo("User already exists: %s", req.Email)
		http.Error(w, "User already exists", http.StatusConflict)
		return
	}

	//  Verify email format
	ok = validator.Matches(req.Email, validator.EmailRX)
	if !ok {
		a.logInfo("Invalid email format: %s", req.Email)
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

	a.logDebug("User registration data: %+v", userRegistrationData)

	response, err := a.authClient.RegisterUser(ctx, userRegistrationData)
	if err != nil {
		span.AddEvent("User registration failed", trace.WithAttributes())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	span.AddEvent("User registered successfully", trace.WithAttributes())
	responseJSON, err := json.Marshal(response)
	if err != nil {
		a.logInfo("Failed to marshal response: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	a.logInfo("User registered successfully: %v", response)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(responseJSON)
	if err != nil {
		a.logInfo("Failed to write response: %v", err)
	}
}

func (a *Application) login(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the login operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "login")
	defer span.End()

	a.logInfo("login handler called")

	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		a.logInfo("Failed to decode JSON body: %v", err)
		http.Error(w, "Failed to decode JSON body", http.StatusBadRequest)
		return
	}

	// Verify Blank fields
	if req.Email == "" || req.Password == "" {
		a.logInfo("One or more required fields are blank")
		http.Error(w, "All fields are required", http.StatusBadRequest)
		return
	}

	//  Check if user exists
	userExistsResp, err := a.authClient.CheckUserExists(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		a.logInfo("Failed to check if user exists: %v", err)
		http.Error(w, "Failed to check if user exists", http.StatusInternalServerError)
		return
	}
	if !userExistsResp.IsUser {
		a.logInfo("User is not registered: %s", req.Email)
		http.Error(w, "User is not registered", http.StatusConflict)
		return
	}

	// Get password hash from auth service
	hashedPassword, err := a.authClient.RetrieveHashedPassword(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		a.logInfo("Failed to retrieve hashed password: %v", err)
		http.Error(w, "Failed to retrieve hashed password", http.StatusInternalServerError)
		return
	}

	// Check password is valid
	if err := bcrypt.CompareHashAndPassword([]byte(hashedPassword.HashedPassword), []byte(req.Password)); err != nil {
		a.logInfo("Invalid password for user %s: %v", req.Email, err)
		http.Error(w, "Invalid password", http.StatusUnauthorized)
		return
	}

	// Generate JWT token
	token, err := a.authClient.GenerateToken(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		a.logInfo("GenerateToken failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}
	a.logDebug("Token generated for user %s: %s", req.Email, token.Jwt)
	a.logInfo("User logged in successfully: %s", req.Email)
	_, err = w.Write([]byte(token.Jwt))
	if err != nil {
		log.Println("Failed to write token:", err)
	}
}

func (a *Application) checkBalance(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the checkBalance operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "checkBalance")
	defer span.End()

	a.logInfo("checkBalance called")

	// Check user has valid JWT Token in Authorisation Header
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logInfo("Authorization failed: %v", err)
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
		log.Printf("Failed to read request body: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logDebug("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		a.logInfo("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logDebug("checkBalance payload: %+v", payload)
	authorizedResponse, err := a.mmClient.CheckBalance(ctx, &mmpb.CheckBalancePayload{
		EmailAddress: payload.EmailAddress,
	})
	if err != nil {
		a.logInfo("Check balance failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}

	a.logDebug("checkBalance response: %+v", authorizedResponse)
	type response struct {
		Cents int64 `json:"cents"`
	}

	resp := response{
		Cents: authorizedResponse.BalanceCents,
	}
	responseJSON, err := json.Marshal(resp)
	if err != nil {
		a.logInfo("Failed to marshal response: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	a.logInfo("Check balance succeeded")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(responseJSON)
	if err != nil {
		a.logInfo("Failed to write response: %v", err)
	}
}

func (a *Application) customerPaymentAuthorize(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the customerPaymentAuthorize operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "customerPaymentAuthorize")
	defer span.End()

	a.logInfo("customerPaymentAuthorize called")

	// Check user has valid JWT Token in Authorisation Header
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logInfo("Authorization failed: %v", err)
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
		log.Printf("Failed to read request body: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logDebug("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		a.logInfo("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logDebug("Authorize payload: %+v", payload)
	authorizedResponse, err := a.mmClient.Authorize(ctx, &mmpb.AuthorizePayload{
		CustomerEmailAddress: payload.CustomerEmailAddress,
		MerchantEmailAddress: payload.MerchantEmailAddress,
		Cents:                payload.Cents,
		Currency:             payload.Currency,
	})
	if err != nil {
		a.logInfo("Money movement authorization failed: %v", err)
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
		a.logInfo("Failed to marshal response: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	a.logInfo("Authorization succeeded")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(responseJSON)
	if err != nil {
		a.logInfo("Failed to write response: %v", err)
	}
}

func (a *Application) customerPaymentCapture(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the customerPaymentCapture operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "customerPaymentCapture")
	defer span.End()

	a.logInfo("customerPaymentCapture handler called")

	// Check user has valid JWT Token in Authorisation Header
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logInfo("Authorization failed: %v", err)
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
		a.logInfo("Failed to read request body: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logDebug("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		a.logInfo("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logDebug("Capture payload: %+v", payload)
	_, err = a.mmClient.Capture(ctx, &mmpb.CapturePayload{Pid: payload.Pid})
	if err != nil {
		a.logInfo("Money movement capture failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}
	a.logInfo("Capture succeeded")
	w.WriteHeader(http.StatusOK)
}

func (a *Application) createAccount(w http.ResponseWriter, r *http.Request) {
	// Start a new span for the createAccount operation
	ctx := r.Context()
	ctx, span := a.tracer.Start(ctx, "createAccount")
	defer span.End()

	a.logInfo("createAccount handler called")

	// Check user has valid JWT Token in Authorisation Header
	err := a.checkAuthHeader(ctx, r)
	if err != nil {
		a.logInfo("Authorization failed: %v", err)
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
		log.Printf("Failed to read request body: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	a.logDebug("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		a.logInfo("Failed to unmarshal payload: %v", err)
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
		a.logInfo("Invalid WalletType: %v", payload.WalletType)
		http.Error(w, "wallet_type must be CUSTOMER or MERCHANT", http.StatusBadRequest)
		return
	}

	// Validate JWT token user is the same as payload.EmailAddress

	// Create account
	a.logDebug("Create Account payload: %+v", payload)
	_, err = a.mmClient.CreateAccount(ctx, &mmpb.CreateAccountPayload{
		EmailAddress:           payload.EmailAddress,
		WalletType:             walletTypeEnum,
		InitialBalanceCents:    payload.InitialBalanceCents,
		InitialBalanceCurrency: payload.InitialBalanceCurrency,
	})
	if err != nil {
		a.logInfo("Create Account failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}
	a.logInfo("Create Account succeeded")
	w.WriteHeader(http.StatusOK)
}

func (a *Application) checkAuthHeader(context context.Context, r *http.Request) error {
	authHeader := r.Header.Get("Authorization")
	a.logDebug("Authorization header: %s", authHeader)
	if authHeader == "" {
		a.logInfo("Missing Authorization header")
		return status.Error(codes.Unauthenticated, "missing Authorization header")
	}
	if !strings.HasPrefix(authHeader, "Bearer ") {
		a.logInfo("Authorization header does not start with 'Bearer '")
		return status.Error(codes.Unauthenticated, "invalid Authorization header")
	}
	token := strings.TrimPrefix(authHeader, "Bearer ")
	a.logDebug("Extracted token: %s", token)

	if token == "" {
		a.logInfo("Token is empty after trimming 'Bearer ' prefix")
		return status.Error(codes.Unauthenticated, "empty token")
	}

	// Validate the token
	email, err := a.validateToken(context, token)
	if err != nil {
		a.logInfo("Token validation failed: %v", err)
		return status.Error(codes.Unauthenticated, "invalid token")
	}

	// Token is valid
	a.logDebug("%s Token  is valid", email.UserEmail)
	return nil
}

func (a *Application) validateToken(context context.Context, token string) (*authpb.UserEmailAddress, error) {
	// Start a new span for the validateToken operation
	ctx, span := a.tracer.Start(context, "validateToken")
	defer span.End()

	email, err := a.authClient.VerifyToken(ctx, &authpb.Token{Jwt: token})
	if err != nil {
		a.logInfo("Token validation failed: %v", err)
		return email, status.Error(codes.Unauthenticated, "invalid token")
	}
	return email, nil
}
