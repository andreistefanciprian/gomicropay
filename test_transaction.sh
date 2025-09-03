#!/bin/bash

# Generate a unique email address for each test run

# Arrays of possible emails
CUSTOMER_EMAILS=("customer1@email.com" "customer2@email.com" "customer3@email.com")
MERCHANT_EMAILS=("merchant1@email.com" "merchant2@email.com" "merchant3@email.com")

# Pick a random email from each array

# Pick a random email from each array
CUSTOMER_EMAIL_ADDRESS=${CUSTOMER_EMAILS[$RANDOM % ${#CUSTOMER_EMAILS[@]}]}
MERCHANT_EMAIL_ADDRESS=${MERCHANT_EMAILS[$RANDOM % ${#MERCHANT_EMAILS[@]}]}

# Big echo showing selected emails for this test run
echo
echo "============================================================="
echo "[TEST INIT] Using CUSTOMER_EMAIL_ADDRESS: $CUSTOMER_EMAIL_ADDRESS"
echo "[TEST INIT] Using MERCHANT_EMAIL_ADDRESS: $MERCHANT_EMAIL_ADDRESS"
echo "============================================================="

# Funcs
register_user() {
    echo
    echo "========== [Step 1] Registering user =========="
    echo "Using email: $CUSTOMER_EMAIL_ADDRESS"
    REGISTER_PAYLOAD="{\"first_name\": \"Happy\", \"last_name\": \"Man\", \"email\": \"$CUSTOMER_EMAIL_ADDRESS\", \"password\": \"SecurePass123!\"}"
    echo "Register payload: $REGISTER_PAYLOAD"
    REGISTER_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" -d "$REGISTER_PAYLOAD" http://localhost:8080/register)
    echo "Register response: $REGISTER_RESPONSE"
}

login_user() {
    echo
    echo "========== [Step 2] Logging in and generating JWT token =========="
    LOGIN_PAYLOAD="{\"email\": \"$CUSTOMER_EMAIL_ADDRESS\", \"password\": \"SecurePass123!\"}"
    echo "Login payload: $LOGIN_PAYLOAD"
    JWT_TOKEN=$(curl -s -X POST -H "Content-Type: application/json" -d "$LOGIN_PAYLOAD" http://localhost:8080/login)
    echo "JWT token response: $JWT_TOKEN"
}

create_customer_account() {
    echo
    echo "========== [Step 3] Create CUSTOMER account =========="
    CREATE_ACCOUNT_PAYLOAD="{\"email_address\": \"$CUSTOMER_EMAIL_ADDRESS\", \"wallet_type\": \"CUSTOMER\", \"initial_balance_cents\": 4000000, \"initial_balance_currency\": \"USD\"}"
    echo "Create Customer Accunt payload: $CREATE_ACCOUNT_PAYLOAD"
    curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" -H "Content-Type: application/json" -d "$CREATE_ACCOUNT_PAYLOAD" http://localhost:8080/create-account
}

create_merchant_account() {
    echo
    echo "========== [Step 4] Create MERCHANT account =========="
    CREATE_ACCOUNT_PAYLOAD="{\"email_address\": \"$MERCHANT_EMAIL_ADDRESS\", \"wallet_type\": \"MERCHANT\", \"initial_balance_cents\": 0, \"initial_balance_currency\": \"USD\"}"
    echo "Create Merchant Account payload: $CREATE_ACCOUNT_PAYLOAD"
    curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" -H "Content-Type: application/json" -d "$CREATE_ACCOUNT_PAYLOAD" http://localhost:8080/create-account
}

authorize_payment() {
    echo
    echo "========== [Step 5] Authorizing payment =========="
    AUTHORIZE_PAYLOAD="{\"customer_email_address\": \"$CUSTOMER_EMAIL_ADDRESS\", \"merchant_email_address\": \"$MERCHANT_EMAIL_ADDRESS\", \"cents\": 1000, \"currency\": \"USD\"}"
    echo "Authorize payload: $AUTHORIZE_PAYLOAD"
    pid=$(curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" -H "Content-Type: application/json" -d "$AUTHORIZE_PAYLOAD" http://localhost:8080/customer/payment/authorize | jq -r .pid)
    echo "Payment authorized. PID: $pid"
}

capture_payment() {
    echo
    echo "========== [Step 6] Capturing payment =========="
    CAPTURE_PAYLOAD="{\"pid\": \"$pid\"}"
    echo "Capture payload: $CAPTURE_PAYLOAD"
    CAPTURE_RESPONSE=$(curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" -d "$CAPTURE_PAYLOAD" http://localhost:8080/customer/payment/capture)
    echo "Capture response: $CAPTURE_RESPONSE"
}

check_balance() {
    echo
    echo "========== [Step 7] Checking balance =========="
    BALANCE_PAYLOAD="{\"email_address\": \"$CUSTOMER_EMAIL_ADDRESS\"}"
    echo "Balance payload: $BALANCE_PAYLOAD"
    BALANCE_RESPONSE=$(curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" -d "$BALANCE_PAYLOAD" http://localhost:8080/checkbalance)
    echo "Balance response: $BALANCE_RESPONSE"
}

check_databases() {
    echo
    echo "========== [Step 8] Checking Databases =========="

    # Check contents of auth.registered_users table
    echo
    echo "========== [DB] auth.registered_users =========="
    docker run -it --network host --rm mysql mysql -h127.0.0.1 -P 33061 -u root -pAdmin123 -e 'select * from auth.registered_users;'

    # Check contents of money_movement tables
    echo
    echo "========== [DB] money_movement.wallet, account, transaction =========="
    docker run -it --network host --rm mysql mysql -h127.0.0.1 -P 33062 -u root -pAdmin123 -e \
        'select * from money_movement.wallet; select * from money_movement.account; select * from money_movement.transaction;'

    # Check contents of ledger.ledger table
    echo
    echo "========== [DB] ledger.ledger =========="
    docker run -it --network host --rm mysql mysql -h127.0.0.1 -P 33063 -u root -pAdmin123 -e 'select * from ledger.ledger;'
}

# Call steps in order
register_user
login_user
create_customer_account
create_merchant_account
authorize_payment
capture_payment
check_balance
check_databases