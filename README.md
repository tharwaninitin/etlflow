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

| Module Name        | Scala 2.12           | Scala 2.13  | Scala 3.0  | 
| -------------------|:--------------------:| -----------:| ----------:|
| Utils              | ✅                   | ✅          | ✅          |
| Json               | ✅                   | ✅          | ✅          |
| Cache              | ✅                   | ✅          | ✅          |
| Crypto             | ✅                   | ✅          | ✅          |
| Db                 | ✅                   | ✅          | ✅          |
| Core               | ✅                   | ✅          | ✅          |
| Server             | ✅                   | ✅          | ✅          |
| Http               | ✅                   | ✅          | ✅          |
| Cloud              | ✅                   | ✅          | ✅          |
| Email              | ✅                   | ✅          | ✅          |
| Aws                | ✅                   | ✅          | ✅          |
| Gcp                | ✅                   | ✅          | ✅          |
| Redis              | ✅                   | ✅          | ❌          |
| Spark              | ✅                   | ✅          | ❌          |


## Examples 

You can use this library in different ways mentioned below

* [Core Module](examplecore) :         
    Using this module you can use Step API into your project
* [Spark Module with spark steps](examplespark) :         
  Using this module you can use spark steps into your project
* [Job Module](examplejob) :         
    Using this module you can use Job API into your project
* [Server Module](exampleserver) :         
    Using this module you can use Server API into your project.

## Requirements and Installation
This project is compiled with scala versions 2.12.15, 2.13.7, 3.1.0
Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core).
Add the latest release as a dependency to your project

[![Latest Version](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

__SBT__
```
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-core" % "x.x.x"
libraryDependencies += "com.github.tharwaninitin" %% "etlflow-job" % "x.x.x"
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

## Contributions
Please feel free to add issues to report any bugs or to propose new features.
