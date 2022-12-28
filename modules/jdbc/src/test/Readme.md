#Set up database
```shell
docker run -it --rm \
-p 5432:5432 \
-e POSTGRES_PASSWORD=etlflow \
-e POSTGRES_USER=etlflow \
-e POSTGRES_DB=etlflow \
postgres:13-alpine
```

#Set environment variables for PG
```shell
export DB_URL=jdbc:postgresql://localhost:5432/etlflow
export DB_USER=etlflow
export DB_PWD=etlflow
export DB_DRIVER=org.postgresql.Driver
export INIT=true
```

#Set environment variables for MYSQL
```shell
export DB_URL=jdbc:mysql://localhost:3306/etlflow?allowMultiQueries=true
export DB_USER=etlflow
export DB_PWD=etlflow
export DB_DRIVER=com.mysql.cj.jdbc.Driver
export INIT=true
```

#Run Sample Test Job
```shell
sbt ";project db; Test/runMain etlflow.audit.CreateDB"
sbt ";project db; Test/runMain etlflow.SampleJobWithDbLogging"
```

#Run Tests
```shell
sbt ";project db; test"
```
