#Set up database
```shell
docker run -it --rm \
-p 5432:5432 \
-e POSTGRES_PASSWORD=etlflow \
-e POSTGRES_USER=etlflow \
-e POSTGRES_DB=etlflow \
postgres:13-alpine
```

#Set environment variables
```shell
export LOG_DB_URL=jdbc:postgresql://localhost:5432/etlflow
export LOG_DB_USER=etlflow
export LOG_DB_PWD=etlflow
export LOG_DB_DRIVER=org.postgresql.Driver
```

#Run Tests
```shell
sbt ";project db; +test"
```
