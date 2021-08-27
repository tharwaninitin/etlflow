## QuickStart
To be able to use this library with database logging of job and steps you need postgres instance up and running, then create new database in pg (for e.g. etlflow)

STEP 1) Create the application.conf file at location examplecore/src/main/resources and set the below variables:
```shell
  db.url      = ${LOG_DB_URL},
  db.user     = ${LOG_DB_USER},
  db.password = ${LOG_DB_PWD},
  db.driver   = ${LOG_DB_DRIVER}

  timezone    = "Asia/Kolkata"

  slack.url   = "http://slackwebhookurl",
  slack.env   = "dev"
  slack.host  = "http://localhost:8080/#"
```

STEP 2) set below environment variables in your shell.
```shell
 export LOG_DB_URL=jdbc:postgresql://localhost:5432/etlflow
 export LOG_DB_USER=<...>
 export LOG_DB_PWD=<..>
 export LOG_DB_DRIVER=org.postgresql.Driver
```

STEP 3) Now to create database tables used in this library run below commands from repo root folder:
```shell
sbt
project exampleserver
runMain examples.RunApp initdb
```

STEP 4) To list commands available to run use below:
```shell
sbt
project exampleserver
runMain examples.RunApp --help
```

STEP 5) To list available jobs:
```shell
sbt
project exampleserver
runMain examples.RunApp list_jobs
```

STEP 6) Now to run sample job use below command:
```shell
sbt
project exampleserver
runMain examples.RunApp run_job --job_name JobDBSteps
```

STEP 7) Add user in database for using UI:
```shell
sbt
project exampleserver
runMain examples.RunApp add_user --user admin --password admin
```

STEP 8) Now run below command to run server http://localhost:8080:
```shell
sbt
project exampleserver
runMain examples.RunApp run_server
```