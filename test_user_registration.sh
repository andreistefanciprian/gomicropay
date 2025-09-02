#!/bin/bash

# Registering user
echo "Registering user..."
curl -X POST --data @user_reg_form.json http://localhost:8080/register
echo

# User login
echo "Generating JWT token..."
JWT_TOKEN=$(curl -s X POST  --data @user_login.json http://localhost:8080/login)
echo "JWT_TOKEN: $JWT_TOKEN"
echo

# Authorize payment and capture pid from response
echo "Authorizing payment..."
pid=$(curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" --data @authorize_payload.json http://localhost:8080/customer/payment/authorize | jq -r .pid)
echo "pid: $pid"
echo

# Capture payment using pid
echo "Capturing payment..."
curl -X POST -H "Authorization: Bearer $JWT_TOKEN" -d "{\"pid\": \"$pid\"}" http://localhost:8080/customer/payment/capture

# Check balance
echo "Checking balance..."
curl -X POST -H "Authorization: Bearer $JWT_TOKEN" -d "{\"wallet_user_id\": \"cip@email.com\"}" http://localhost:8080/checkbalance