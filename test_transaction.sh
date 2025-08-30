#!/bin/bash

# Generate JWT token for user
echo "Generating JWT token..."
JWT_TOKEN=$(curl -s -u cip@email.com:Admin123 http://localhost:8080/login)
echo "JWT_TOKEN: $JWT_TOKEN"
sleep 1

# Authorize payment and capture pid from response
echo "Authorizing payment..."
pid=$(curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" --data @authorize_payload.json http://localhost:8080/customer/payment/authorize | jq -r .pid)
echo "pid: $pid"
sleep 1

# Capture payment using pid
echo "Capturing payment..."
curl -X POST -H "Authorization: Bearer $JWT_TOKEN" -d "{\"pid\": \"$pid\"}" http://localhost:8080/customer/payment/capture

# Check balance
echo "Checking balance..."
curl -X POST -H "Authorization: Bearer $JWT_TOKEN" -d "{\"wallet_user_id\": \"cip@email.com\"}" http://localhost:8080/checkbalance