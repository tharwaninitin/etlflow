## QuickStart

### Without application.conf file

STEP 1) To be able to use this library, add below dependency in sbt file : 
```
  "com.github.tharwaninitin" %% "etlflow-core" % EtlFlowVersion,
  "ch.qos.logback" % "logback-classic" % LogbackVersion
```

STEP 2) To list commands available to run use below:
```shell
sbt
project examples
runMain examples.LoadData --help
```

STEP 3) To list available jobs:
```shell
sbt
project examples
runMain examples.LoadData list_jobs
```

STEP 4) Now to run sample job use below command:
```shell
sbt
project examples
runMain examples.LoadData run_job --job_name PARQUETtoJDBC
```

### With application.conf file

STEP 1) To be able to use this library, first you need postgres instance up and running, then create new database in pg (for e.g. etlflow), then set below environment variables.
```shell
 export LOG_DB_URL=jdbc:postgresql://localhost:5432/etlflow
 export LOG_DB_USER=<...>
 export LOG_DB_PWD=<..>
 export LOG_DB_DRIVER=org.postgresql.Driver
```

STEP 2) Create the application.conf file at location exampleserver/src/main/resources and set the below variables:
```shell
  db.url      = ${LOG_DB_URL},
  db.user     = ${LOG_DB_USER},
  db.password = ${LOG_DB_PWD},
  db.driver   = ${LOG_DB_DRIVER}

  timezone = "Asia/Kolkata"

  slack.url = "",
  slack.env = "dev"
  slack.host = ""

  dataproc.mainclass = "examples.LoadData"
  dataproc.deplibs = []
```

STEP 2) Now to create database tables used in this library run below commands from repo root folder:
```shell
sbt
project examples
runMain examples.LoadData run_db_migration
```

STEP 3) To list commands available to run use below:
```shell
sbt
project examples
runMain examples.LoadData --help
```

STEP 4) To list available jobs:
```shell
sbt
project examples
runMain examples.LoadData list_jobs
```

STEP 5) Now to run sample job use below command:
```shell
sbt
project examples
runMain examples.LoadData run_job --job_name PARQUETtoJDBC
```
