# EtlFlow

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![EtlFlow CI](https://github.com/tharwaninitin/etlflow/actions/workflows/ci.yml/badge.svg)](https://github.com/tharwaninitin/etlflow/actions/workflows/ci.yml)
[![Semantic Versioning Policy Check](https://github.com/tharwaninitin/etlflow/actions/workflows/semver.yml/badge.svg)](https://github.com/tharwaninitin/etlflow/actions/workflows/semver.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)
[![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-core_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-core_2.12)

**EtlFlow** is an ecosystem of functional libraries in Scala based on ZIO for writing various different tasks, jobs on GCP and AWS.

[//]: # (## Documentation)

[//]: # ()
[//]: # (__Library Documentation__  https://tharwaninitin.github.io/etlflow/site/docs)

[//]: <> (__Scala Test Coverage Report__  https://tharwaninitin.github.io/etlflow/testcovrep/)

## Examples
* [Core Module](examples/examplecore):         
  In this example project, you can explore core features of etlflow, Task and Audit API.
* [Spark Module (Spark tasks)](examples/examplespark):         
  In this example project, you can explore Apache Spark tasks.
* [GCP Module (GCS, DataProc, BigQuery tasks)](examples/examplegcp):         
  In this example project, you can explore GCP tasks.

## Modules Dependency Graph

![ModuleDepGraph](moduleDep.png)

| Module | Latest Version                                                                                                                                                                                         |                                                                                                                                                       Documentation | Scala Versions                                                                                                                                                                                           | 
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Core   | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)   |   [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-core_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-core_2.12) | [![etlflow-core Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-core/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-core)    |
| Gcp    | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-gcp_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-gcp)     |     [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-gcp_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-gcp_2.12) | [![etlflow-gcp Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-gcp/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-gcp)       |
| Db     | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-db_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-db)       |       [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-db_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-db_2.12) | [![etlflow-db Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-db/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-db)          |
| K8S    | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-k8s_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-k8s)     |     [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-k8s_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-k8s_2.12) | [![etlflow-k8s Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-k8s/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-k8s)       |
| Http   | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-http_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-http)   |   [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-http_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-http_2.12) | [![etlflow-http Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-http/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-http)    |
| Email  | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-email_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-email) | [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-email_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-email_2.12) | [![etlflow-email Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-email/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-email) |
| Aws    | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-aws_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-aws)     |     [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-aws_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-aws_2.12) | [![etlflow-aws Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-aws/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-aws)       |
| Redis  | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-redis_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-redis) | [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-redis_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-redis_2.12) | [![etlflow-redis Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-redis/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-redis) |
| Spark  | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-spark_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-spark) | [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-spark_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-spark_2.12) | [![etlflow-spark Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-spark/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-spark) |

## Requirements and Installation
This project is compiled with scala versions 2.12.17, 2.13.10, 3.2.0

Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core).
Add the below latest release as a dependency to your project

[![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

__SBT__
```scala
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-core" % "@VERSION@"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-spark" % "@VERSION@"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-gcp" % "@VERSION@"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-k8s" % "@VERSION@"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-http" % "@VERSION@"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-redis" % "@VERSION@"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-aws" % "@VERSION@"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-email" % "@VERSION@"
```
__Maven__
```
<dependency>
    <groupId>com.github.tharwaninitin</groupId>
    <artifactId>etlflow-core_2.12</artifactId>
    <version>@VERSION@</version>
</dependency>
```

# Etlflow Modules
<!-- TOC -->
- [Etlflow Modules](#etlflow-modules)
  - [Core](#core)
  - [Gcp](#gcp)
  - [Db](#db)
  - [K8S](#k8s)
  - [Http](#http)
  - [Email](#email)
  - [Aws](#aws)
  - [Redis](#redis)
  - [Spark](#spark)
<!-- /TOC -->

## Core
```scala mdoc:silent
// Todo
```
## GCP
```scala mdoc:silent
// Todo
```
## DB
```scala mdoc:silent
// Todo
```
## K8s
```scala mdoc:silent
// Todo
```
## Http
```scala mdoc:silent
// Todo
```
## Email
```scala mdoc:silent
// Todo
```
## AWS
```scala mdoc:silent
// Todo
```
## Redis
```scala mdoc:silent
// Todo
```
## Spark
```scala mdoc:silent
// Todo
```

## Contributions
Please feel free to add issues to report any bugs or to propose new features.