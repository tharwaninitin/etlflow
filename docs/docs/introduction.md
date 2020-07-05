---
layout: docs
title: Introduction
---

## Introduction

This library provides abstraction on top of Apache Spark, No SQL Databases, Relational Databases and multiple different datasources that makes it easier to develop **ETL applications** which can be easily Tested and Composed together. 
This library has many **plug-and-play steps** that are ready to handle your task on **Google Cloud Platform(GCP)** and **Amazon Web Services(AWS)**.

**What we are doing Earlier** : 
* **Maintenance** : Earlier we are maintaining etl ingestion library which is written in scala and Apache airflow which is scheduling tool written in python.
* **Two Level Dependency** : Every time when developer writes new ingestion job he needs to create airflow dag for that job.So there is always two level dependancy developers are facing every time.
* **Difficult to Maintain Single Source Code** : Some logic written in scala and some in python which makes it very difficult for developers to maintain single source.
* **Time Consuming Process** : In case of production issues/update we need to change in both airflow dags and ingestion jobs so two level of testing we need to perform.

**Below are the few disadvantages of Apache Airflow** : 
* Dag Code maintenance
* To perform the basic task we need to use airflow operators.
* Need to learn python programming language for scheduling jobs.

But with the help og newly developed etlflow library we can now overcome the above disadvantges.

**What we are doing Now** :
* **Maintenance** : Maintaining Single source of code. 
* **Less Deployment** : Less deployment time as only library deployment we need to perform.
* **In Build Webserver** : Provides environment like airflow includes both scheduler and webserver. Performing all basic tasks that airflow is providing.
* **Single Source Code** : All logic can be written in scala.
* **Single Level Dependency** : Single level dependency as we replaced airflow with etlflow in built library.
* **Plug-and-play Process** : Just Plug-and-play steps.
* **Support of Various Services** : Provides abstraction on top of Apache Spark, No SQL Databases, Relational Databases and multiple different datasources that makes it easier to develop.
* **Cloud Support** : Support of Google Cloud Platform(GCP) and mazon Web Services(AWS)

**Advantages of ETLFLOW library** :
* This library is a single, near-zero maintenance platform delivered as-a-service.
* Features compute, storage, and cloud services layers steps that are logically integrated but scale independent from one another, making it an ideal platform for many workloads.
* It is also a powerful query processing back-end library for developers creating modern data-driven applications.
* This library is a combination of webserver and scheduler which can easily replace the apache airlfow tool.
* Easily deployable in a cloud environment.
* Ease of Use
* Fully automated and Generic library