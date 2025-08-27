

deploy-mysql:
	@kubectl apply -f mysql_auth/manifests/. -n kafka
	@kubectl apply -f mysql_ledger/manifests/. -n kafka
	@kubectl apply -f mysql_money_movement/manifests/. -n kafka

undeploy-mysql:
	@kubectl delete -f mysql_auth/manifests/. -n kafka
	@kubectl delete -f mysql_ledger/manifests/. -n kafka
	@kubectl delete -f mysql_money_movement/manifests/. -n kafka

# api_gateway
docker-gateway: 
	@docker build -t andreistefanciprian/gomicropay-gateway:latest -f api_gateway/Dockerfile api_gateway/
	@docker push andreistefanciprian/gomicropay-gateway:latest
deploy-gateway:
	@kubectl apply -f api_gateway/manifests/. -n kafka
undeploy-gateway:
	@kubectl delete -f api_gateway/manifests/. -n kafka

# auth
docker-auth:
	@docker build -t andreistefanciprian/gomicropay-auth:latest -f auth/Dockerfile auth/
	@docker push andreistefanciprian/gomicropay-auth:latest
deploy-auth:
	@kubectl apply -f auth/manifests/. -n kafka
undeploy-auth:
	@kubectl delete -f auth/manifests/. -n kafka

# money_movement
docker-money-movement:
	@docker build -t andreistefanciprian/gomicropay-money-movement:latest -f money_movement/Dockerfile money_movement/
	@docker push andreistefanciprian/gomicropay-money-movement:latest
deploy-money-movement:
	@kubectl apply -f money_movement/manifests/. -n kafka
undeploy-money-movement:
	@kubectl delete -f money_movement/manifests/. -n kafka

# email
docker-email:
	@docker build -t andreistefanciprian/gomicropay-email:latest -f email/Dockerfile email/
	@docker push andreistefanciprian/gomicropay-email:latest
deploy-email:
	@kubectl apply -f email/manifests/. -n kafka
undeploy-email:
	@kubectl delete -f email/manifests/. -n kafka

# ledger
docker-ledger:
	@docker build -t andreistefanciprian/gomicropay-ledger:latest -f ledger/Dockerfile ledger/
	@docker push andreistefanciprian/gomicropay-ledger:latest
deploy-ledger:
	@kubectl apply -f ledger/manifests/. -n kafka
undeploy-ledger:
	@kubectl delete -f ledger/manifests/. -n kafka


# all
deploy-all: deploy-mysql deploy-auth deploy-email deploy-money-movement deploy-gateway deploy-ledger
undeploy-all: undeploy-auth undeploy-email undeploy-money-movement undeploy-gateway undeploy-mysql undeploy-ledger

