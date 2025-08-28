
#### Test
```

make deploy-all

kubectl port-forward gateway 8080:8080 -n kafka

JWT_TOKEN=$(curl -s -u cip@email.com:Admin123 http://localhost:8080/login)

curl -X POST -H "Authorization: Bearer $JWT_TOKEN" --data @authorize_payload.json http://localhost:8080/customer/payment/authorize
curl -X POST -H "Authorization: Bearer $JWT_TOKEN" --data @capture_payload.json http://localhost:8080/customer/payment/capture

# debug mysql
k apply -f mysql_money_movement/manifests/mysql-client-pod.yaml -n kafka
k exec -ti mysql-client -- mysql -h mysql-money-movement -u money_movement_user -p

```