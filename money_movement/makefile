gen:
	@protoc \
	--go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	./proto/money_movement_svc.proto


docker: docker-build docker-push

docker-build:
	@docker build -t andreistefanciprian/gomicropay-money-movement:latest .

docker-push:
	@docker push andreistefanciprian/gomicropay-money-movement:latest

deploy:
	@kubectl apply -f manifests/.

undeploy:
	@kubectl delete -f manifests/.