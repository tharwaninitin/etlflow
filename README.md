EtlFlow
====

![Tests](https://github.com/tharwaninitin/etlflow/workflows/Tests/badge.svg)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)
[![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-core_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-core_2.12)

**EtlFlow** is a Functional library in Scala for writing ETL jobs.

## Documentation

__Library Documentation__  https://tharwaninitin.github.io/etlflow/site/

__Scala Test Coverage Report__  https://tharwaninitin.github.io/etlflow/testcovrep/

## Running Tests
All the tests are integration tests. That is, they make real API requests to S3, GCS, BigQuery. 
As such, you'll need to make sure you have variables set to a bucket and object that you can access and manipulate.

Here are all the environment variables you will need to set to run the tests locally:

 ```shell
 export LOG_DB_URL=jdbc:postgresql://localhost:5432/etlflow
 export LOG_DB_USER=<...>
 export LOG_DB_PWD=<..>
 export LOG_DB_DRIVER=org.postgresql.Driver

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
 sbt "project core" test
 ```

## Requirements and Installation
This project is compiled with scala version 2.12.10 and works with Apache Spark versions 2.4.x.
Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core). 
Add the latest release as a dependency to your project

[![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

__Maven__
```
<dependency>
    <groupId>com.github.tharwaninitin</groupId>
    <artifactId>etlflow-core_2.12</artifactId>
    <version>x.x.x</version>
</dependency>
```
__SBT__
```
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-core" % "x.x.x"
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

STEP 3) Now to run sample job use below command:
```shell
sbt
project examples
runMain examples.LoadData run_job --job_name EtlJob2DefinitionLocal
```

#### Contributions
Please feel free to add issues to report any bugs or to propose new features.
