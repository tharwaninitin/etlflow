---
layout: home
title:  "Home"
section: "home"
technologies:
 - first: ["Scala", "Most amazing JVM language"]
 - second: ["Spark", "Apache Spark™ is a unified analytics engine for large-scale data processing."]
 - third: ["Zio", "Type-safe, composable asynchronous and concurrent programming for Scala"]
 - fourth: ["GCP", "Google Cloud Platform"]
---

At a very high level, the library provides abstraction on top of Apache Spark, No SQL Databases, Relational Databases and multiple different datasources that makes it easier to develop ETL applications which can be easily Tested and Composed. 
This library has many plug-and-play steps that are ready to handle your task on Google Cloud Platform (Support for other cloud providers coming soon). This makes EtlFlow easy to use with your current infrastructure.

## Requirements and Installation

This project is compiled with scala version 2.12.10 and works with Apache Spark versions 2.4.x. Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core). Add the latest release as a dependency to your project

**Maven**

    <dependency>
        <groupId>com.github.tharwaninitin</groupId>
        <artifactId>etlflow-core_2.12</artifactId>
        <version>0.7.10</version>
    </dependency>
    
**SBT**

    libraryDependencies += "com.github.tharwaninitin" %% "etlflow-core" % "0.7.10"

[Download Latest](https://github.com/tharwaninitin/etlflow/releases/tag/v0.7.10)
 

