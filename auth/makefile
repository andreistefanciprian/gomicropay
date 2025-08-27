gen:
	@protoc \
	--go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	./proto/auth_svc.proto

docker: docker-build docker-push

docker-build:
	@docker build -t andreistefanciprian/gomicropay-auth:latest .

docker-push:
	@docker push andreistefanciprian/gomicropay-auth:latest

deploy:
	@kubectl apply -f manifests/.