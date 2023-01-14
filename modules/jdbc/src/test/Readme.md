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

#Set environment variables for H2
```shell
export DB_URL=jdbc:h2:file:~/h2db/coordinator
export DB_USER=etlflow
export DB_PWD=etlflow
export DB_DRIVER=org.h2.Driver
export INIT=true

rm -r ~/h2db # Cleanup directory before running
```

#Create DB Schema
```shell
sbt ";project jdbc; Test/runMain etlflow.audit.CreateDB"
```

#Run Tests
```shell
sbt ";project jdbc; test"
```

#Run Sample Test Job
```shell
sbt ";project jdbc; Test/runMain etlflow.SampleJobWithDbLogging"
```


