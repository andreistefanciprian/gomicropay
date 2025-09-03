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
	"github.com/andreistefanciprian/gomicropay/api_gateway/internal/validator"
	mmpb "github.com/andreistefanciprian/gomicropay/api_gateway/money_movement/proto"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var mmClient mmpb.MoneyMovementServiceClient
var authClient authpb.AuthServiceClient

var logLevel string

func logDebug(format string, v ...interface{}) {
	if logLevel == "DEBUG" {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func logInfo(format string, v ...interface{}) {
	if logLevel == "INFO" || logLevel == "DEBUG" {
		log.Printf("[INFO] "+format, v...)
	}
}

func main() {

	authHost := os.Getenv("AUTH_HOST")
	authPort := os.Getenv("AUTH_PORT")
	moneyMovementHost := os.Getenv("MONEY_MOVEMENT_HOST")
	moneyMovementPort := os.Getenv("MONEY_MOVEMENT_PORT")
	moneyMovementAddress := fmt.Sprintf("%s:%s", moneyMovementHost, moneyMovementPort)
	authAddress := fmt.Sprintf("%s:%s", authHost, authPort)

	logLevel = os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "INFO"
	}
	logInfo("Starting API Gateway with log level: %s", logLevel)

	// auth connection
	authConn, err := grpc.NewClient(authAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := authConn.Close(); err != nil {
			log.Println("Failed to close auth connection:", err)
		}
	}()

	authClient = authpb.NewAuthServiceClient(authConn)

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
	mmClient = mmpb.NewMoneyMovementServiceClient(mmConn)

	http.HandleFunc("/register", register)
	http.HandleFunc("/login", login)
	http.HandleFunc("/create-account", createAccount)
	http.HandleFunc("/customer/payment/authorize", customerPaymentAuthorize)
	http.HandleFunc("/customer/payment/capture", customerPaymentCapture)
	http.HandleFunc("/checkbalance", checkBalance)

	log.Println("API Gateway listening on port 8080")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}

func register(w http.ResponseWriter, r *http.Request) {
	logInfo("register handler called")

	var req struct {
		FirstName string `json:"first_name"`
		LastName  string `json:"last_name"`
		Email     string `json:"email"`
		Password  string `json:"password"`
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		logInfo("Failed to decode JSON body: %v", err)
		http.Error(w, "Failed to decode JSON body", http.StatusBadRequest)
		return
	}

	// Verify Blank fields
	if req.FirstName == "" || req.LastName == "" || req.Email == "" || req.Password == "" {
		logInfo("One or more required fields are blank")
		http.Error(w, "All fields are required", http.StatusBadRequest)
		return
	}

	// Verify password length
	ok := validator.MinChars(req.Password, 8)
	if !ok {
		logInfo("Password does not meet minimum length requirement")
		http.Error(w, "Password must be at least 8 characters long", http.StatusBadRequest)
		return
	}

	// Create a bcrypt hash of the plain-text password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
	if err != nil {
		logInfo("Failed to hash password: %v", err)
		http.Error(w, "Failed to hash password", http.StatusInternalServerError)
		return
	}

	//  Check if user exists
	userExistsResp, err := authClient.CheckUserExists(context.Background(), &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		logInfo("Failed to check if user exists: %v", err)
		http.Error(w, "Failed to check if user exists", http.StatusInternalServerError)
		return
	}
	if userExistsResp.IsUser {
		logInfo("User already exists: %s", req.Email)
		http.Error(w, "User already exists", http.StatusConflict)
		return
	}

	//  Verify email format
	ok = validator.Matches(req.Email, validator.EmailRX)
	if !ok {
		logInfo("Invalid email format: %s", req.Email)
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

	logDebug("User registration data: %+v", userRegistrationData)

	response, err := authClient.RegisterUser(context.Background(), userRegistrationData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		logInfo("Failed to marshal response: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	logInfo("User registered successfully: %v", response)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(responseJSON)
	if err != nil {
		logInfo("Failed to write response: %v", err)
	}
}

func login(w http.ResponseWriter, r *http.Request) {
	logInfo("userLogin handler called")

	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		logInfo("Failed to decode JSON body: %v", err)
		http.Error(w, "Failed to decode JSON body", http.StatusBadRequest)
		return
	}

	// Verify Blank fields
	if req.Email == "" || req.Password == "" {
		logInfo("One or more required fields are blank")
		http.Error(w, "All fields are required", http.StatusBadRequest)
		return
	}

	//  Check if user exists
	userExistsResp, err := authClient.CheckUserExists(context.Background(), &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		logInfo("Failed to check if user exists: %v", err)
		http.Error(w, "Failed to check if user exists", http.StatusInternalServerError)
		return
	}
	if !userExistsResp.IsUser {
		logInfo("User is not registered: %s", req.Email)
		http.Error(w, "User is not registered", http.StatusConflict)
		return
	}

	// Get password hash from auth service
	hashedPassword, err := authClient.RetrieveHashedPassword(context.Background(), &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		logInfo("Failed to retrieve hashed password: %v", err)
		http.Error(w, "Failed to retrieve hashed password", http.StatusInternalServerError)
		return
	}

	// Check password is valid
	if err := bcrypt.CompareHashAndPassword([]byte(hashedPassword.HashedPassword), []byte(req.Password)); err != nil {
		logInfo("Invalid password for user %s: %v", req.Email, err)
		http.Error(w, "Invalid password", http.StatusUnauthorized)
		return
	}

	// Generate JWT token
	ctx := context.Background()
	token, err := authClient.GenerateToken(ctx, &authpb.UserEmailAddress{UserEmail: req.Email})
	if err != nil {
		logInfo("GenerateToken failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}
	logDebug("Token generated for user %s: %s", req.Email, token.Jwt)
	logInfo("User logged in successfully: %s", req.Email)
	_, err = w.Write([]byte(token.Jwt))
	if err != nil {
		log.Println("Failed to write token:", err)
	}
}

func checkBalance(w http.ResponseWriter, r *http.Request) {
	logInfo("checkBalance called")

	// Check user has valid JWT Token in Authorisation Header
	ctx := context.Background()
	err := checkAuthHeader(ctx, r)
	if err != nil {
		logInfo("Authorization failed: %v", err)
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

	logDebug("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		logInfo("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	logDebug("checkBalance payload: %+v", payload)
	ctx = context.Background()
	authorizedResponse, err := mmClient.CheckBalance(ctx, &mmpb.CheckBalancePayload{
		EmailAddress: payload.EmailAddress,
	})
	if err != nil {
		logInfo("Check balance failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}

	logDebug("checkBalance response: %+v", authorizedResponse)
	type response struct {
		Cents int64 `json:"cents"`
	}

	resp := response{
		Cents: authorizedResponse.BalanceCents,
	}
	responseJSON, err := json.Marshal(resp)
	if err != nil {
		logInfo("Failed to marshal response: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	logInfo("Check balance succeeded")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(responseJSON)
	if err != nil {
		logInfo("Failed to write response: %v", err)
	}
}

func customerPaymentAuthorize(w http.ResponseWriter, r *http.Request) {
	logInfo("customerPaymentAuthorize called")

	// Check user has valid JWT Token in Authorisation Header
	ctx := context.Background()
	err := checkAuthHeader(ctx, r)
	if err != nil {
		logInfo("Authorization failed: %v", err)
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

	logDebug("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		logInfo("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	logDebug("Authorize payload: %+v", payload)
	ctx = context.Background()
	authorizedResponse, err := mmClient.Authorize(ctx, &mmpb.AuthorizePayload{
		CustomerEmailAddress: payload.CustomerEmailAddress,
		MerchantEmailAddress: payload.MerchantEmailAddress,
		Cents:                payload.Cents,
		Currency:             payload.Currency,
	})
	if err != nil {
		logInfo("Money movement authorization failed: %v", err)
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
		logInfo("Failed to marshal response: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	logInfo("Authorization succeeded")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(responseJSON)
	if err != nil {
		logInfo("Failed to write response: %v", err)
	}
}

func customerPaymentCapture(w http.ResponseWriter, r *http.Request) {
	logInfo("customerPaymentCapture handler called")

	// Check user has valid JWT Token in Authorisation Header
	ctx := context.Background()
	err := checkAuthHeader(ctx, r)
	if err != nil {
		logInfo("Authorization failed: %v", err)
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
		logInfo("Failed to read request body: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	logDebug("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		logInfo("Failed to unmarshal payload: %v", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	logDebug("Capture payload: %+v", payload)
	ctx = context.Background()
	_, err = mmClient.Capture(ctx, &mmpb.CapturePayload{Pid: payload.Pid})
	if err != nil {
		logInfo("Money movement capture failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}
	logInfo("Capture succeeded")
	w.WriteHeader(http.StatusOK)
}

func createAccount(w http.ResponseWriter, r *http.Request) {
	logInfo("createAccount handler called")

	// Check user has valid JWT Token in Authorisation Header
	ctx := context.Background()
	err := checkAuthHeader(ctx, r)
	if err != nil {
		logInfo("Authorization failed: %v", err)
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

	logDebug("Request body: %s", string(body))
	err = json.Unmarshal(body, &payload)
	if err != nil {
		logInfo("Failed to unmarshal payload: %v", err)
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
		logInfo("Invalid WalletType: %v", payload.WalletType)
		http.Error(w, "wallet_type must be CUSTOMER or MERCHANT", http.StatusBadRequest)
		return
	}

	// Validate JWT token user is the same as payload.EmailAddress

	// Create account
	logDebug("Create Account payload: %+v", payload)
	ctx = context.Background()
	_, err = mmClient.CreateAccount(ctx, &mmpb.CreateAccountPayload{
		EmailAddress:           payload.EmailAddress,
		WalletType:             walletTypeEnum,
		InitialBalanceCents:    payload.InitialBalanceCents,
		InitialBalanceCurrency: payload.InitialBalanceCurrency,
	})
	if err != nil {
		logInfo("Create Account failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}
	logInfo("Create Account succeeded")
	w.WriteHeader(http.StatusOK)
}

func checkAuthHeader(context context.Context, r *http.Request) error {
	authHeader := r.Header.Get("Authorization")
	logDebug("Authorization header: %s", authHeader)
	if authHeader == "" {
		logInfo("Missing Authorization header")
		return status.Error(codes.Unauthenticated, "missing Authorization header")
	}

	if !strings.HasPrefix(authHeader, "Bearer ") {
		logInfo("Authorization header does not start with 'Bearer '")
		return status.Error(codes.Unauthenticated, "invalid Authorization header")
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	logDebug("Extracted token: %s", token)
	_, err := authClient.VerifyToken(context, &authpb.Token{Jwt: token})
	if err != nil {
		logInfo("Token verification failed: %v", err)
		return status.Error(codes.Unauthenticated, "invalid token")
	}
	return nil
}
