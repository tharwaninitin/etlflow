## Without database logging
Task 1) To list commands available to run use below:
```shell
sbt
project examplecore
runMain examples.Job1
runMain examples.Job2
runMain examples.Job3
```

## With database logging
To be able to use this library with database logging of job and tasks you need postgres instance up and running, then create new database in pg (for e.g. etlflow)

Task 1) Create the application.conf file at location examplecore/src/main/resources and set the below variables:
```shell
  db.url      = ${LOG_DB_URL},
  db.user     = ${LOG_DB_USER},
  db.password = ${LOG_DB_PWD},
  db.driver   = ${LOG_DB_DRIVER}
```

TASK 2) set below environment variables in your shell.
```shell
 export LOG_DB_URL=jdbc:postgresql://localhost:5432/etlflow
 export LOG_DB_USER=<...>
 export LOG_DB_PWD=<..>
 export LOG_DB_DRIVER=org.postgresql.Driver
```

TASK 3) Now to create database tables used in this library run below commands from repo root folder:
```shell
sbt
project examplecore
runMain etlflow.InitDB
```

TASK 6) Now to run sample job use below command:
```shell
sbt
project examplejob
runMain examples.Job4
```

## With additional slack logging
Add slack properties in application.conf
```shell
  db.url      = ${LOG_DB_URL},
  db.user     = ${LOG_DB_USER},
  db.password = ${LOG_DB_PWD},
  db.driver   = ${LOG_DB_DRIVER}

  slack.url   = "http://slackwebhookurl",
  slack.env   = "dev"
  slack.host  = "http://localhost:8080/#"
```
