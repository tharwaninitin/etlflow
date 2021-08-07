EtlFlow
====

[![Core Workflow](https://github.com/tharwaninitin/etlflow/actions/workflows/core.yml/badge.svg)](https://github.com/tharwaninitin/etlflow/actions/workflows/core.yml)
[![Server Workflow](https://github.com/tharwaninitin/etlflow/actions/workflows/server.yml/badge.svg)](https://github.com/tharwaninitin/etlflow/actions/workflows/server.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)
[![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-core_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-core_2.12)

**EtlFlow** is a Functional library in Scala for writing ETL jobs.

## Documentation

__Library Documentation__  https://tharwaninitin.github.io/etlflow/site/docs

[comment]: <> (__Scala Test Coverage Report__  https://tharwaninitin.github.io/etlflow/testcovrep/)

## Modules Dependency Graph

![Example](moduleDep.png)

## Scala Version Compatibility Matrix

| Module Name        | Scala 2.12           | Scala 2.13  | Scala 3    | 
| -------------------|:--------------------:| -----------:| ----------:|
| Utils              | ✅                   | ✅          | ✅          |
| Json               | ✅                   | ✅          | ✅          |
| crypto             | ✅                   | ✅          | ✅          |
| cache              | ✅                   | ✅          | ✅          |
| Db                 | ✅                   | ✅          | ❌          |
| Core               | ✅                   | ✅          | ❌          |
| Redis              | ✅                   | ✅          | ❌          |
| Http               | ✅                   | ✅          | ❌          |
| Cloud              | ✅                   | ✅          | ❌          |
| Server             | ✅                   | ✅          | ❌          |
| email              | ✅                   | ✅          | ❌          |
| aws                | ✅                   | ✅          | ❌          |
| gcp                | ✅                   | ✅          | ❌          |
| Spark              | ✅                   | ❌          | ❌          |


## Requirements and Installation
This project is compiled with scala versions 2.12.13, 2.13.6, 3.0.0
Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core).
Add the latest release as a dependency to your project

[![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

__SBT__
```
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-core" % "x.x.x"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-server" % "x.x.x"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-spark" % "x.x.x"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-cloud" % "x.x.x"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-http" % "x.x.x"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-redis" % "x.x.x"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-aws" % "x.x.x"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-gcp" % "x.x.x"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-email" % "x.x.x"

```
__Maven__
```
<dependency>
    <groupId>com.github.tharwaninitin</groupId>
    <artifactId>etlflow-core_2.12</artifactId>
    <version>x.x.x</version>
</dependency>
```

## QuickStart
STEP 1) To be able to use this library, first you need postgres instance up and running, then create new database in pg (for e.g. etlflow), then set below environment variables.
 ```shell
 export LOG_DB_URL=jdbc:postgresql://localhost:5432/etlflow
 export LOG_DB_USER=<...>
 export LOG_DB_PWD=<..>
 export LOG_DB_DRIVER=org.postgresql.Driver
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
runMain examples.LoadData -l
```

STEP 5) Now to run sample job use below command:
```shell
sbt
project examples
runMain examples.LoadData run_job --job_name PARQUETtoJDBC
```

STEP 6) Add user in database for using UI:
```shell
sbt
project examples
runMain examples.LoadData add_user --user admin --password admin
```

STEP 7) Now run below command to run server http://localhost:8080:
```shell
sbt
project examples
runMain examples.RunServer 
```

## Running Tests
There are multiple modules and all contains tests. To be able to use this library, first you need postgres instance up and running, then create new database in pg (for e.g. etlflow), then set below environment variables.
 ```shell
 export LOG_DB_URL=jdbc:postgresql://localhost:5432/etlflow
 export LOG_DB_USER=<...>
 export LOG_DB_PWD=<..>
 export LOG_DB_DRIVER=org.postgresql.Driver
```
Now to create database tables used in this library run below commands from repo root folder:
```shell
sbt
project core
test:runMain etlflow.LoadData run_db_migration
```
### Core Module
To test core module run below command:
```shell
sbt
project core
test
```
### Cloud Module
All the tests in this module are integration tests. That is, they make real API requests to S3, GCS, BigQuery.
As such, you'll need to make sure you have variables set to a bucket and object that you can access and manipulate.

Here are all the environment variables you will need to set to run the tests locally:

 ```shell
 export GOOGLE_APPLICATION_CREDENTIALS=<...> # this should be full path to GCP Service Account Key Json which should have GCS and BigQuery Read/Write access
 export GCS_BUCKET=<...> 
 export GCS_INPUT_LOCATION=<...>
 export PUBSUB_SUBSCRIPTION=<...>

 export ACCESS_KEY=<...>
 export SECRET_KEY=<...>
 export S3_BUCKET=<...>
 export S3_INPUT_LOCATION=<...>

 export DP_PROJECT_ID=<...>
 export DP_REGION=<...>
 export DP_ENDPOINT=<...>
 export DP_CLUSTER_NAME=<...>
 export DP_JOB_NAME=SampleJob2LocalJob
 export DP_MAIN_CLASS=examples.LoadData
 export DP_LIBS=gs://<bucket_name>/core/etlflow-core-assembly-x.x.x.jar,gs://<bucket_name>/core/etlflow-examples_2.12-x.x.x.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar,gs://<bucket_name>/core/postgresql-42.2.8.jar
```
Change the region in TestSuiteHelper.scala to your region in AWS for s3 bucket.
You also would need docker installed as some of the tests start/stop database docker containers

Now run tests using below sbt command
 ```shell
 sbt "project cloud" test
 ```

#### Contributions
Please feel free to add issues to report any bugs or to propose new features.
