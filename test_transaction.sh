#!/bin/bash

# Generate a unique email address for each test run
# EMAIL_ADDRESS="gigi$(date +%s)$RANDOM@email.com"
EMAIL_ADDRESS="cip@email.com"

register_user() {
    echo
    echo "========== [Step 1] Registering user =========="
    echo "Using email: $EMAIL_ADDRESS"
    REGISTER_PAYLOAD="{\"first_name\": \"Gigi\", \"last_name\": \"Gheorghe\", \"email\": \"$EMAIL_ADDRESS\", \"password\": \"SecurePass123!\"}"
    echo "Register payload: $REGISTER_PAYLOAD"
    REGISTER_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" -d "$REGISTER_PAYLOAD" http://localhost:8080/register)
    echo "Register response: $REGISTER_RESPONSE"
}

login_user() {
    echo
    echo "========== [Step 2] Logging in and generating JWT token =========="
    LOGIN_PAYLOAD="{\"email\": \"$EMAIL_ADDRESS\", \"password\": \"SecurePass123!\"}"
    echo "Login payload: $LOGIN_PAYLOAD"
    JWT_TOKEN=$(curl -s -X POST -H "Content-Type: application/json" -d "$LOGIN_PAYLOAD" http://localhost:8080/login)
    echo "JWT token response: $JWT_TOKEN"
}

authorize_payment() {
    echo
    echo "========== [Step 3] Authorizing payment =========="
    AUTHORIZE_PAYLOAD="{\"customer_wallet_user_id\": \"$EMAIL_ADDRESS\", \"merchant_wallet_user_id\": \"merchant_id\", \"cents\": 1000, \"currency\": \"USD\"}"
    echo "Authorize payload: $AUTHORIZE_PAYLOAD"
    pid=$(curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" -H "Content-Type: application/json" -d "$AUTHORIZE_PAYLOAD" http://localhost:8080/customer/payment/authorize | jq -r .pid)
    echo "Payment authorized. PID: $pid"
}

capture_payment() {
    echo
    echo "========== [Step 4] Capturing payment =========="
    CAPTURE_PAYLOAD="{\"pid\": \"$pid\"}"
    echo "Capture payload: $CAPTURE_PAYLOAD"
    CAPTURE_RESPONSE=$(curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" -d "$CAPTURE_PAYLOAD" http://localhost:8080/customer/payment/capture)
    echo "Capture response: $CAPTURE_RESPONSE"
}

check_balance() {
    echo
    echo "========== [Step 5] Checking balance =========="
    BALANCE_PAYLOAD="{\"wallet_user_id\": \"$EMAIL_ADDRESS\"}"
    echo "Balance payload: $BALANCE_PAYLOAD"
    BALANCE_RESPONSE=$(curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" -d "$BALANCE_PAYLOAD" http://localhost:8080/checkbalance)
    echo "Balance response: $BALANCE_RESPONSE"
}

# Call steps in order
register_user
login_user
authorize_payment
capture_payment
check_balance