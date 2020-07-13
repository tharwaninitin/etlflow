Setting Up Docker Environment
====

## Step 1 (Starting Local Docker Environment)
docker-compose --compatibility up -d

## Step 2 (Verify Environment)
docker-compose --compatibility ps

docker-compose --compatibility top

## Step 3 (Browse UI)
Application http://0.0.0.0:8080/

Prometheus http://0.0.0.0:9090/

Grafana http://0.0.0.0:3000/

## Step 4 (Setting Up Grafana Dashboard)
1) Add new prometheus datasource with url **prometheus:9090**
2) Import dashboard **grafana_jvm.json** from current directory