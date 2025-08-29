

## Requirements

- Docker
- GNU Make
- Kubernetes (local or remote cluster)
- Kafka (installed via [Strimzi](https://strimzi.io/quickstarts/))

## Microservice Architecture Demo

This repository is a hands-on way to learn about microservice architecture using gRPC and Kafka. It demonstrates service-to-service communication, event-driven workflows, and database integration in a modern distributed system.

![Architecture Overview](microservices_architecture.jpg)

## Transaction Flow

- The user first sends their login credentials through the REST API Gateway, which forwards the request to the Auth service over gRPC. 
- If the authentication is successful, the Auth service responds with a JWT token.
- From then on, the user includes this token in the request header whenever making a transaction call to the API Gateway. 
- The Gateway checks the token’s validity with the Auth service before moving forward.
- Once authorized, the API Gateway passes the transaction request to the Money Movement service over gRPC. 
- This service debits the user’s DEFAULT account and credits the PAYMENT account, recording the transaction in its database.
- At the same time, the Money Movement service produces two event messages to Kafka, which are then consumed by other services. 
- The Ledger service picks up the event and updates its records, while the Email service sends the user a notification about the transaction.
- Finally, the Money Movement service returns a confirmation back to the user indicating that the transaction was successful.

#### Test
```
# Deploy all services
make deploy-all

# Port-forward API Gateway to localhost:8080
kubectl port-forward service/gateway 8080:8080 -n api-gateway

# Debug MySQL money movement database
kubectlk exec -ti mysql-client -n money-movement -- mysql -h mysql-money-movement -u money_movement_user -p
mysql>use money_movement;
mysql>show tables;
mysql>select * from transaction;

k exec -ti mysql-client -n money-movement -- mysql -h mysql-ledger.ledger -u ledger_user -p

# check logs
kubectl logs -l app=gateway -n api-gateway -f
kubectl logs -l app=money-movement -n money-movement -f
kubectl logs -l app=ledger -n ledger -f
kubectl logs -l app=auth -n auth -f
kubectl logs -l app=email -n email -f

# generate transaction
bash test_transaction.sh

# Remove all services
make undeploy-all
```