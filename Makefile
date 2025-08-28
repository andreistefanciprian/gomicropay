
# api_gateway
docker-gateway: 
	@docker build -t andreistefanciprian/gomicropay-gateway:latest -f api_gateway/infra/Dockerfile api_gateway/
	@docker push andreistefanciprian/gomicropay-gateway:latest
deploy-gateway:
	@kustomize build api_gateway/infra/k8s | kubectl apply -f -
undeploy-gateway:
	@kustomize build api_gateway/infra/k8s | kubectl delete -f -

# auth
proto-auth:
	@protoc \
	--go_out=paths=source_relative:. \
	--go-grpc_out=paths=source_relative:. \
	./auth/proto/auth_svc.proto

docker-auth:
	@docker build -t andreistefanciprian/gomicropay-auth:latest -f auth/infra/Dockerfile auth/
	@docker push andreistefanciprian/gomicropay-auth:latest
deploy-auth:
	@kustomize build auth/infra/k8s | kubectl apply -f -
undeploy-auth:
	@kustomize build auth/infra/k8s | kubectl delete -f -

# money_movement
proto-money-movement:
	@protoc \
	--go_out=paths=source_relative:. \
	--go-grpc_out=paths=source_relative:. \
	./money_movement/proto/money_movement_svc.proto
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
	@docker build -t andreistefanciprian/gomicropay-ledger:latest -f ledger/infra/Dockerfile ledger/
	@docker push andreistefanciprian/gomicropay-ledger:latest
deploy-ledger:
	@kustomize build ledger/infra/k8s | kubectl apply -f -
undeploy-ledger:
	@kustomize build ledger/infra/k8s | kubectl delete -f -

# all
deploy-all: deploy-auth deploy-email deploy-money-movement deploy-gateway deploy-ledger
undeploy-all: undeploy-auth undeploy-email undeploy-money-movement undeploy-gateway undeploy-ledger

