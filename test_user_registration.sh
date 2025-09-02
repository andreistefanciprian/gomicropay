#!/bin/bash

# Registering user
echo "Registering user..."
curl -X POST --data @user_reg_form.json http://localhost:8080/register
