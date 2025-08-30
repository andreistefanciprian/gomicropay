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

	authpb "github.com/andreistefanciprian/gomicropay/api_gateway/auth"
	mmpb "github.com/andreistefanciprian/gomicropay/api_gateway/money_movement"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	http.HandleFunc("/login", login)
	http.HandleFunc("/customer/payment/authorize", customerPaymentAuthorize)
	http.HandleFunc("/customer/payment/capture", customerPaymentCapture)
	http.HandleFunc("/checkbalance", checkBalance)

	log.Println("API Gateway listening on port 8080")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}

func login(w http.ResponseWriter, r *http.Request) {
	logInfo("login handler called")
	username, password, ok := r.BasicAuth()
	logDebug("BasicAuth username: %s", username)
	if !ok {
		logInfo("BasicAuth failed: missing or invalid credentials")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	ctx := context.Background()
	token, err := authClient.GetToken(ctx, &authpb.Credentials{
		UserName: username,
		Password: password,
	})
	if err != nil {
		logInfo("GetToken failed: %v", err)
		_, writeErr := w.Write([]byte(err.Error()))
		if writeErr != nil {
			log.Println(writeErr)
		}
		return
	}
	logDebug("Token generated for user %s: %s", username, token.Jwt)
	_, err = w.Write([]byte(token.Jwt))
	if err != nil {
		log.Println("Failed to write token:", err)
	}
}

func checkBalance(w http.ResponseWriter, r *http.Request) {
	logInfo("checkBalance called")
	authHeader := r.Header.Get("Authorization")
	logDebug("Authorization header: %s", authHeader)
	if authHeader == "" {
		logInfo("Missing Authorization header")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	if !strings.HasPrefix(authHeader, "Bearer ") {
		logInfo("Authorization header does not start with 'Bearer '")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	logDebug("Extracted token: %s", token)
	ctx := context.Background()
	_, err := authClient.ValidateToken(ctx, &authpb.Token{Jwt: token})
	if err != nil {
		logInfo("Token validation failed: %v", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// Call money movement service
	type checkBalancePayload struct {
		WalletUserId string `json:"wallet_user_id"`
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
		UserId: payload.WalletUserId,
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
	authHeader := r.Header.Get("Authorization")
	logDebug("Authorization header: %s", authHeader)
	if authHeader == "" {
		logInfo("Missing Authorization header")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	if !strings.HasPrefix(authHeader, "Bearer ") {
		logInfo("Authorization header does not start with 'Bearer '")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	logDebug("Extracted token: %s", token)
	ctx := context.Background()
	_, err := authClient.ValidateToken(ctx, &authpb.Token{Jwt: token})
	if err != nil {
		logInfo("Token validation failed: %v", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	// Call money movement service
	type authorizePayload struct {
		CustomerWalletUserId string `json:"customer_wallet_user_id"`
		MerchantWalletUserId string `json:"merchant_wallet_user_id"`
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
		CustomerWalletUserId: payload.CustomerWalletUserId,
		MerchantWalletUserId: payload.MerchantWalletUserId,
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
	authHeader := r.Header.Get("Authorization")
	logDebug("Authorization header: %s", authHeader)
	if authHeader == "" {
		logInfo("Missing Authorization header")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	if !strings.HasPrefix(authHeader, "Bearer ") {
		logInfo("Authorization header does not start with 'Bearer '")
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	logDebug("Extracted token: %s", token)
	ctx := context.Background()
	_, err := authClient.ValidateToken(ctx, &authpb.Token{Jwt: token})
	if err != nil {
		logInfo("Token validation failed: %v", err)
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
