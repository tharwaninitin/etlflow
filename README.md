# EtlFlow

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![EtlFlow CI](https://github.com/tharwaninitin/etlflow/actions/workflows/ci.yml/badge.svg)](https://github.com/tharwaninitin/etlflow/actions/workflows/ci.yml)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)
[![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-core_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-core_2.12)

**EtlFlow** is an ecosystem of functional libraries in Scala based on ZIO for running complex **_Auditable_** workflows which can interact with GCP, AWS, Kubernetes, Databases and more.

Below are some salient features of this library.

- **Functional**. Rapidly compose complex **workflows** from simple tasks.
- **Fibers**. Built on non-blocking fibers that never waste or leak resources, which lets you build scalable, resilient, and reactive applications
- **Resource-safe**. Build workflows that never leak resources (including threads!), even when they fail.
- **Concurrent**. Easily build concurrent workflows without deadlocks, race conditions, or complexity.
- **Asynchronous**. Write sequential code that looks the same whether it's asynchronous or synchronous.
- **Type-safe**. Use the full power of the Scala compiler to catch bugs at compile time.
- **Testable**. Inject test services into your workflow for fast, deterministic, and type-safe testing.
- **Resilient**. Build workflows that never lose errors, and which respond to failure locally and flexibly.

[//]: # (## Documentation)
[//]: # ()
[//]: # (__Library Documentation__  https://tharwaninitin.github.io/etlflow/site/docs)
[//]: <> (__Scala Test Coverage Report__  https://tharwaninitin.github.io/etlflow/testcovrep/)

## Examples
* [Core Module](examples/examplecore):         
  In this example project, you can explore core features of etlflow, Task and Audit API.
* [GCP Module (GCS, DataProc, BigQuery tasks)](examples/examplegcp):         
  In this example project, you can explore GCP tasks.
* [K8S Module (K8S tasks)](examples/examplek8s):         
  In this example project, you can explore different Kubernetes tasks.
* [Spark Module (Spark tasks)](examples/examplespark):         
  In this example project, you can explore different Apache Spark tasks.

## Modules Dependency Graph

![ModuleDepGraph](moduleDep.png)

| Module | Latest Version                                                                                                                                                                                         |                                                                                                                                                       Documentation | Scala Versions                                                                                                                                                                                           | Java Version | 
|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| Core   | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)   |         [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-core_3/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-core_3) | [![etlflow-core Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-core/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-core)    | Java 8 +     |
| Gcp    | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-gcp_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-gcp)     |           [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-gcp_3/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-gcp_3) | [![etlflow-gcp Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-gcp/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-gcp)       | Java 8 +     |
| JDBC   | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-jdbc_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-jdbc)   |         [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-jdbc_3/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-jdbc_3) | [![etlflow-jdbc Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-jdbc/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-jdbc)    | Java 8 +     |
| Http   | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-http_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-http)   |         [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-http_3/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-http_3) | [![etlflow-http Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-http/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-http)    | Java 11 +    |
| K8S    | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-k8s_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-k8s)     |           [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-k8s_3/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-k8s_3) | [![etlflow-k8s Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-k8s/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-k8s)       | Java 8 +     |
| Email  | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-email_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-email) |       [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-email_3/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-email_3) | [![etlflow-email Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-email/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-email) | Java 8 +     |
| Aws    | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-aws_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-aws)     |           [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-aws_3/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-aws_3) | [![etlflow-aws Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-aws/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-aws)       | Java 8 +     |
| Redis  | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-redis_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-redis) | [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-redis_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-redis_2.12) | [![etlflow-redis Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-redis/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-redis) | Java 8 +     |
| Spark  | [![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-spark_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-spark) | [![javadoc](https://javadoc.io/badge2/com.github.tharwaninitin/etlflow-spark_2.12/javadoc.svg)](https://javadoc.io/doc/com.github.tharwaninitin/etlflow-spark_2.12) | [![etlflow-spark Scala version support](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-spark/latest-by-scala-version.svg)](https://index.scala-lang.org/tharwaninitin/etlflow/etlflow-spark) | Java 8 +     |

## Requirements and Installation
This project is compiled with scala versions 2.12.17, 2.13.10, 3.2.1

Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core).
Add the below latest release as a dependency to your project

[![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

__SBT__
```scala
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-core" % "1.7.0"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-gcp" % "1.7.0"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-jdbc" % "1.7.0"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-spark" % "1.7.0"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-k8s" % "1.7.0"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-http" % "1.7.0"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-redis" % "1.7.0"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-aws" % "1.7.0"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-email" % "1.7.0"
```
__Maven__
```
<dependency>
    <groupId>com.github.tharwaninitin</groupId>
    <artifactId>etlflow-core_2.12</artifactId>
    <version>1.7.0</version>
</dependency>
```

# Etlflow Modules
<!-- TOC -->
- [Etlflow Modules](#etlflow-modules)
  - [Core](#core)
    - [Task](#task)
    - [Audit](#audit)
    - [Config](#config)
    - [Json](#json)
  - [GCP](#gcp)
    - [Dataproc](#dataproc)
  - [K8S](#k8s)
  - [JDBC](#jdbc)
  - [Http](#http)
  - [Email](#email)
  - [Aws](#aws)
  - [Redis](#redis)
  - [Spark](#spark)
<!-- /TOC -->

## Core
```scala
// Todo
```
### Task
```scala
// Todo
```
### Audit
```scala
// Todo
```
### Config
```scala
// Todo
```
### Json
```scala
// Todo
```
## GCP
```shell
# To run all below GCP examples set the GOOGLE_APPLICATION_CREDENTIALS environment variable to the location of the service account json key. 
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```
### Dataproc
```scala
import etlflow.task._
import gcp4zio.dp._
import etlflow.audit
import etlflow.audit.Audit
import zio._

val gcpProject: String = "GCP_PROJECT"
val gcpRegion: String  = "GCP_REGION"
val dpCluster: String  = "DP_CLUSTER"
val dpEndpoint: String = "DP_ENDPOINT"
val dpBucket: String   = "DP_BUCKET"

val createCluster = DPCreateTask("DPCreateTask", dpCluster, ClusterProps(dpBucket)).toZIO
val deleteCluster = DPDeleteTask("DPDeleteTask", dpCluster).toZIO

val args      = List("1000")
val mainClass = "org.apache.spark.examples.SparkPi"
val libs      = List("file:///usr/lib/spark/examples/jars/spark-examples.jar")
val conf      = Map("spark.executor.memory" -> "1g", "spark.driver.memory" -> "1g")

val sparkJob = DPSparkJobTask("DPSparkJobTask", args, mainClass, libs, conf).toZIO

val programGCP: RIO[DPJob with DPCluster with Audit, Unit] = for {
  _ <- createCluster
  _ <- sparkJob
  _ <- deleteCluster
} yield ()

val dpJobLayer = DPJob.live(dpCluster, gcpProject, gcpRegion, dpEndpoint)
val dpClusterLayer = DPCluster.live(gcpProject, gcpRegion, dpEndpoint)

programGCP.provide(dpJobLayer ++ dpClusterLayer ++ audit.noop)
```
Check [this](examples/examplegcp/src/main/scala/examples/Job1GCP.scala) for complete example.

## K8S
```scala
import etlflow.task._
import etlflow.k8s._
import etlflow.audit
import etlflow.audit.Audit
import zio._

val jobName: String = "hello"

val programK8S: RIO[K8S with Audit, Unit] = for {
  _ <- K8SJobTask(
    name = "CreateKubeJobTask",
    jobName = jobName,
    image = "busybox:1.28",
    command = Some(List("/bin/sh", "-c", "sleep 5; ls /etc/key; date; echo Hello from the Kubernetes cluster"))
  ).toZIO
  _ <- K8STrackJobTask("TrackKubeJobTask", jobName).toZIO
  _ <- K8SJobLogTask("GetKubeJobLogTask", jobName).toZIO
  _ <- K8SDeleteJobTask("DeleteKubeJobTask", jobName).toZIO
} yield ()

programK8S.provide(K8S.live() ++ audit.noop)
```
Check [this](examples/examplek8s/src/main/scala/examples/Job1K8S.scala) for complete example.
## JDBC
```scala
// Todo
```
## Http
```scala
// Todo
```
## Email
```scala
// Todo
```
## AWS
```scala
// Todo
```
## Redis
```scala
// Todo
```
## Spark
```scala
// Todo
```

## Contributions
Please feel free to add issues to report any bugs or to propose new features.