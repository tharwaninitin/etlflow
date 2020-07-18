---
layout: home
title:  "Home"
section: "home"
technologies:
 - first: ["Scala", "Most amazing JVM language"]
 - second: ["Spark", "Apache Spark™ is a unified analytics engine for large-scale data processing."]
 - third: ["ZIO", "Type-safe, composable asynchronous and concurrent programming for Scala"]
 - fourth: ["GCP", "Google Cloud Platform"]
 - fifth: ["AWS", "Amazon Web Services"]
---
 
This library provides **plug-and-play steps** for Apache Spark, No SQL Databases(Redis, BigQuery etc), Relational Databases(Postgres, Mysql etc), Cloud Storage(S3, GCS etc) and multiple different libraries/datasources that makes it easier to develop **ETL applications** in **Scala** which can be easily Tested and Composed together. These **steps** are ready to handle various tasks on **Google Cloud Platform(GCP)** and **Amazon Web Services(AWS)**.

## Requirements and Installation

This project is compiled with scala version 2.12.10 and works with Apache Spark versions 2.4.x. Available via [maven central](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core). Add the latest release as a dependency to your project

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

**Maven**

    <dependency>
        <groupId>com.github.tharwaninitin</groupId>
        <artifactId>etlflow-core_2.12</artifactId>
        <version>x.x.x</version>
    </dependency>
    
**SBT**

    libraryDependencies += "com.github.tharwaninitin" %% "etlflow-core" % "x.x.x"

 

