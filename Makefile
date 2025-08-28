

deploy-mysql:
	@kubectl apply -f mysql_auth/manifests/. -n kafka
	@kubectl apply -f mysql_ledger/manifests/. -n kafka

undeploy-mysql:
	@kubectl delete -f mysql_auth/manifests/. -n kafka
	@kubectl delete -f mysql_ledger/manifests/. -n kafka

# api_gateway
docker-gateway: 
	@docker build -t andreistefanciprian/gomicropay-gateway:latest -f api_gateway/infra/Dockerfile api_gateway/
	@docker push andreistefanciprian/gomicropay-gateway:latest
deploy-gateway:
	@kustomize build api_gateway/infra/k8s | kubectl apply -f -
undeploy-gateway:
	@kustomize build api_gateway/infra/k8s | kubectl delete -f -

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
	@docker build -t andreistefanciprian/gomicropay-money-movement:latest -f money_movement/infra/Dockerfile money_movement/
	@docker push andreistefanciprian/gomicropay-money-movement:latest
deploy-money-movement:
	@kustomize build money_movement/infra/k8s | kubectl apply -f -
undeploy-money-movement:
	@kustomize build money_movement/infra/k8s | kubectl delete -f -

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

