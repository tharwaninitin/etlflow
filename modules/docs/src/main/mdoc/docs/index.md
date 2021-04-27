---
layout: docs
title: Introduction
---

## Introduction

This library provides **plug-and-play steps** for Apache Spark, No SQL Databases(Redis, BigQuery etc), Relational Databases(Postgres, Mysql etc), Cloud Storage(S3, GCS etc) and multiple different libraries/datasources that makes it easier to develop **ETL applications** in **Scala** which can be easily Tested and Composed together. These **steps** are ready to handle various tasks on **Google Cloud Platform(GCP)** and **Amazon Web Services(AWS)**.

## Etlflow Core Features

* **Job and Step Api**
    This library provides job and step api using which we can write very complex ETL pipelines.

* **InBuilt Logging**
    Provides database and slack logging for detailed logging of each step inside job.

* **Single Source Code**
    All logic for entire job can be written in Scala including defining cron schedule, job properties and job definition.  

* **Many plug-and-play steps**
    Provides many steps that are ready to handle your task on Google Cloud Platform(GCP) and Amazon Web Services(AWS).

* **Simple Custom Step API**
    New custom steps can be added using very simple API.  

## Etlflow Server Features

* **InBuilt Rest API**
    Provides feature rich GraphQL api which is tightly integrated with core library, no need to install it separately as plugin. Jobs can be triggered externally for e.g. from GCP Cloud functions using this api.

* **InBuilt Web-Server**
    Server component in this libarary Provides basic UI like airflow which includes both scheduler and webserver, providing all basic functionality that airflow is providing.

## Sample job using Etlflow library

![Example](etlflow.png)

* **Sensor Step:**  
  1. Using [Sensor Step](https://tharwaninitin.github.io/etlflow/site/docs/sensors.html) we can lookup for specified file in a bucket repeatedly until it is
     found or fail after defined number of retries. 
  2. For example, We can use [S3SensorStep](https://tharwaninitin.github.io/etlflow/site/docs/s3sensor.html), [GCSSensorStep](https://tharwaninitin.github.io/etlflow/site/docs/gcssensor.html). 
  3. These steps require the information on Input bucket, Output Bucket, Retry parameters etc.         
* **Data Transfer Step:** 
  1. Using [Data Transfer Steps](https://tharwaninitin.github.io/etlflow/site/docs/cloud_steps.html) we can transfer the data from AWS to GCS. 
  2. For Data transfer step we can use [CloudStoreSyncStep](https://tharwaninitin.github.io/etlflow/site/docs/cloud_steps.html).
  3. Using above mentioned steps we can transfer data from GCS-to-LOCAL, LOCAL-to-S3, S3-to-LOCAL, GCS-to-LOCAL etc.  
* **Spark Step:** 
  1. Using [Spark Step](https://tharwaninitin.github.io/etlflow/site/docs/spark.html) we can load transformed data into destination(BQ,JDBC,GCS).  
  2. For example, we can use  [SparkReadWriteStep](https://tharwaninitin.github.io/etlflow/site/docs/spark.html), [SparkReadTransformWriteStep](https://tharwaninitin.github.io/etlflow/site/docs/spark.html).
  3. This steps can load the input bucket data, transform the same data and write into destination bucket.
  4. We can load the data from (AWS,GCS,JDBC,BQ) and we can write the data into (GCS,JDBC,BQ) using spark Step.  
* **Success/Failure Step:**
     Using [HTTP](https://tharwaninitin.github.io/etlflow/site/docs/http.html) / [EMAIL](https://tharwaninitin.github.io/etlflow/site/docs/sendmail.html) step we can send the success or failure notifications/emails to other teams.
