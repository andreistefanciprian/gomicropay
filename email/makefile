
docker: docker-build docker-push

docker-build:
	@docker build -t andreistefanciprian/gomicropay-email:latest .

docker-push:
	@docker push andreistefanciprian/gomicropay-email:latest

deploy:
	@kubectl apply -f manifests/. -n kafka

undeploy:
	@kubectl delete -f manifests/. -n kafka