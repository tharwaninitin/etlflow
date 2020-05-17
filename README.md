EtlFlow
====

**EtlFlow** is a library that help with faster ETL development using **Apache Spark**. At a very high level,
the library provides abstraction on top of Spark that makes it easier to develop ETL applications which can be easily **Tested** and **Composed**. This library has **Google Bigquery** support as both ETL source and destination.

This project contains following sub-modules:

1. **modules/core**:
 This module contains core library which defines Scala internal **dsl** that assists with writing **ETL Job** which can be composed as multiple **ETL steps** in a concise manner which facilitates easier **Testing** and reasoning about the entire job. This module also conatins many [test jobs](modules/core/src/test/scala) which contains multiple steps. This core library also contains tests and all jobs uses EtlJob API. To run all test successfully some properties needs to be set in [loaddata.properties](modules/core/src/test/resources/loaddata.properties) or set these properties as ENVIRONMENT variables.
 ```shell
 export GCS_OUTPUT_BUCKET=<...>
 export GCP_PROJECT=<...>
 export GCP_PROJECT_KEY_NAME=<...> # this should be full path to Service Account Key Json which should have GCS and Biguery Read/Write access
 ```
 Optionally set below env variables for logging in PG database which needs to be set before running
 ```shell
  export LOG_DB_URL=<...>
  export LOG_DB_USER=<...>
  export LOG_DB_PWD=<...> 
  ```
 Now run tests using below sbt command
 ```shell
sbt "project etlflow" test
```
2. **modules/core/src/main/scala/etlflow/etlsteps**:
 This package contains all type of ETL Steps that can be created with this library, click [here](modules/core/src/main/scala/etlflow/etlsteps) to see.
3. **modules/examples**:
 This module provides examples of different types of ETL Jobs which can be created with this library, click [here](modules/examples/src/main/scala/examples) to see code.

## Requirements and Installation
This project is compiled with scala version 2.12.10 and works with Apache Spark versions 2.4.x.
Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core). 
Add the latest release as a dependency to your project

__Maven__
```
<dependency>
    <groupId>com.github.tharwaninitin</groupId>
    <artifactId>etlflow-core_2.12</artifactId>
    <version>0.7.7</version>
</dependency>
```
__SBT__
```
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-core" % "0.7.7"
```
__Download Latest JAR__ https://github.com/tharwaninitin/etlflow/releases/tag/v0.7.7


## Documentation

__Documentation__  https://tharwaninitin.github.io/etlflow/site/

__Scala API Docs__ https://tharwaninitin.github.io/etlflow/api/

__Scala Test Coverage Report__  https://tharwaninitin.github.io/etlflow/testcovrep/

#### Contributions
Please feel free to add issues to report any bugs or to propose new features.
