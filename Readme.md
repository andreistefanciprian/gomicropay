

## Requirements

- Docker
- GNU Make
- Kubernetes (local or remote cluster)
- Kafka (installed via [Strimzi](https://strimzi.io/quickstarts/))

## Microservice Architecture Demo

This repository is a hands-on way to learn about microservice architecture using gRPC and Kafka. It demonstrates service-to-service communication, event-driven workflows, and database integration in a modern distributed system.

![Architecture Overview](architecture.jpg)

#### Test
```
# Deploy all services
make deploy-all

# Port-forward API Gateway to localhost:8080
kubectl port-forward service/gateway 8080:8080 -n api-gateway

# generate transaction
bash test_transaction.sh

# Debug MySQL money movement database
k apply -f mysql_money_movement/manifests/mysql-client-pod.yaml -n kafka
echo "MySQL client pod created"
sleep 1
k exec -ti mysql-client -- mysql -h mysql-money-movement -u money_movement_user -p

```