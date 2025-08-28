#!/bin/bash

# Generate JWT token for user
JWT_TOKEN=$(curl -s -u cip@email.com:Admin123 http://localhost:8080/login)
echo "JWT_TOKEN: $JWT_TOKEN"
sleep 1

# Authorize payment and capture pid from response
pid=$(curl -s -X POST -H "Authorization: Bearer $JWT_TOKEN" --data @authorize_payload.json http://localhost:8080/customer/payment/authorize | jq -r .pid)
echo "pid: $pid"
sleep 1

# Capture payment using pid
curl -X POST -H "Authorization: Bearer $JWT_TOKEN" -d "{\"pid\": \"$pid\"}" http://localhost:8080/customer/payment/capture