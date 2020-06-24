EtlFlow
====

![Tests](https://github.com/tharwaninitin/etlflow/workflows/Tests/badge.svg)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

**EtlFlow** is a Functional library in Scala for writing ETL jobs.

## Documentation

__Library Documentation__  https://tharwaninitin.github.io/etlflow/site/

__Scala API Docs__ https://tharwaninitin.github.io/etlflow/api/

__Scala Test Coverage Report__  https://tharwaninitin.github.io/etlflow/testcovrep/

## Running Tests
All the tests are integration tests. That is, they make real API requests to S3, GCS, BigQuery. 
As such, you'll need to make sure you have variables set to a bucket and object that you can access and manipulate.

Here are all the things you will need to change to run the tests locally:

 ```shell
 export GOOGLE_APPLICATION_CREDENTIALS=<...> # this should be full path to GCP Service Account Key Json which should have GCS and BigQuery Read/Write access
 export GCS_BUCKET=<...> 
 export ACCESS_KEY=<...>
 export SECRET_KEY=<...>
 export S3_BUCKET=<...>
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

#### Contributions
Please feel free to add issues to report any bugs or to propose new features.
