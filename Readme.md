
#### Test
```
# Deploy all services
make deploy-all

# Port-forward API Gateway to localhost:8080
kubectl port-forward gateway 8080:8080 -n kafka

# generate transaction
bash test_transaction.sh

# Debug MySQL money movement database
k apply -f mysql_money_movement/manifests/mysql-client-pod.yaml -n kafka
echo "MySQL client pod created"
sleep 1
k exec -ti mysql-client -- mysql -h mysql-money-movement -u money_movement_user -p

```