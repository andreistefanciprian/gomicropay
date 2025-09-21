
# api-gateway
docker-gateway: 
	@docker build -t andreistefanciprian/gomicropay-gateway:latest -f api-gateway/infra/Dockerfile api-gateway/
	@docker push andreistefanciprian/gomicropay-gateway:latest
deploy-gateway:
	@kustomize build api-gateway/infra/k8s | kubectl apply -f -
undeploy-gateway:
	@kustomize build api-gateway/infra/k8s | kubectl delete -f -

# auth
proto-auth:
	@protoc \
	--go_out=paths=source_relative:. \
	--go-grpc_out=paths=source_relative:. \
	./auth/proto/auth_svc.proto

	@protoc \
	--go_out=paths=source_relative:./api-gateway/ \
	--go-grpc_out=paths=source_relative:./api-gateway/ \
	./auth/proto/auth_svc.proto
docker-auth:
	@docker build -t andreistefanciprian/gomicropay-auth:latest -f auth/infra/Dockerfile auth/
	@docker push andreistefanciprian/gomicropay-auth:latest
deploy-auth:
	@kustomize build auth/infra/k8s | kubectl apply -f -
undeploy-auth:
	@kustomize build auth/infra/k8s | kubectl delete -f -

# payments
proto-payments:
	@protoc \
	--go_out=paths=source_relative:. \
	--go-grpc_out=paths=source_relative:. \
	./payments/proto/payments_svc.proto

	@protoc \
	--go_out=paths=source_relative:./api-gateway/ \
	--go-grpc_out=paths=source_relative:./api-gateway/ \
	./payments/proto/payments_svc.proto
docker-payments:
	@docker build -t andreistefanciprian/gomicropay-payments:latest -f payments/infra/Dockerfile payments/
	@docker push andreistefanciprian/gomicropay-payments:latest
deploy-payments:
	@kustomize build payments/infra/k8s | kubectl apply -f -
undeploy-payments:
	@kustomize build payments/infra/k8s | kubectl delete -f -

# email
docker-email:
	@docker build -t andreistefanciprian/gomicropay-email:latest -f email/infra/Dockerfile email/
	@docker push andreistefanciprian/gomicropay-email:latest
deploy-email:
	@kustomize build email/infra/k8s | kubectl apply -f -
undeploy-email:
	@kustomize build email/infra/k8s | kubectl delete -f -

# ledger
docker-ledger:
	@docker build -t andreistefanciprian/gomicropay-ledger:latest -f ledger/infra/Dockerfile ledger/
	@docker push andreistefanciprian/gomicropay-ledger:latest
deploy-ledger:
	@kustomize build ledger/infra/k8s | kubectl apply -f -
undeploy-ledger:
	@kustomize build ledger/infra/k8s | kubectl delete -f -

# all
deploy-all: deploy-auth deploy-email deploy-payments deploy-gateway deploy-ledger
undeploy-all: undeploy-auth undeploy-email undeploy-payments undeploy-gateway undeploy-ledger

