---
layout: docs
title: Using with Hadoop Cluster 
---

## Using Etlflow core library in your ETL projects on Hadoop Cluster.

### Prerequisite
* Your system should have one of the below distribution installed machine :
    1. [Cloudera](https://www.cloudera.com/).
    2. [Hortonworks](https://www.cloudera.com/downloads/hortonworks-sandbox.html).
    3. [MapR](https://mapr.com/try-mapr/)
    
* You should have Hadoop cluster created.
* You should appropriate permissions for GCS, BigQuery, Submitting jobs on Hadoop etc

### STEP 1) Building a jar for application, here we will take examples scala project in this library

* To build jar for an application perform below commands from project root folder: 
       
         
      > sbt
      > project examples
      > package
      
* From root folder run 'sbt' command
* Inside sbt, Run 'project examples' to set the current project as examples.
* Once project gets set then run 'package' to build an assembly jar.

### STEP 2) Building an assembly jar for etlflow core
To build an assembly jar for an etlflow core library we need to perform some steps. Clone this git repo and go inside repo root folder and perform below steps: 
       
         
      > sbt
      > project core
      > assembly
      
* Inside root folder run 'sbt' command
* Inside sbt, Run 'project core' to set the current project as etlflow-core
* Once project gets set then run 'assembly' to build an assembly jar.       

### STEP 3) Copy the etlflow core assembly jar to hdfs location  using below command
 
      hdfs dfs -copyFromLocal modules/core/target/scala-2.12/etlflow-core-assembly-x.x.x.jar hdfs://<HDFS_destination>/jars/etlflow
      
Replace x.x.x with the latest version [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

### STEP 4) Copy the examples jar to hdfs location using below command:
 
      hdfs dfs -copyFromLocal  examples/target/scala-2.12/etlflow-examples_2.12-x.x.x.jar  hdfs://<HDFS_destination>/jars/etlflow

Replace x.x.x with the latest version [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)
      
### STEP 5) Provide below detials in file **examples/src/main/resources/application.conf**:


        dbLog = {
          url = <log_db_url>,
          user = <log_db_user>,
          password = <log_db_pwd>,
          driver = "org.postgresql.Driver"
        }
        slack =  {
          url = <slack-url>,
          env = <running-env>
        }
  
* Now Copy updated application.conf file at hdfs location using below command:      
        
        
        hdfs dfs -copyFromLocal examples/src/main/resources/application.conf   hdfs://<HDFS_destination>/jars/etlflow
           
* Copy the input file present at location examples/src/main/data/movies/ratings_parquet/ratings.parquet for running sample job using below command:
    
        
        hdfs dfs -copyFromLocal examples/src/main/data/movies/ratings_parquet/ratings.parquet   hdfs://<HDFS_destination>/jars/etlflow

       
### STEP 6) Now finally we can run sample job on Hadoop cluster using below command: 
  
        spark-submit \
         --master yarn \
         --deploy-mode cluster \
         --driver-memory 8g \
         --executor-memory 16g \
         --class examples.LoadData  \
          hdfs://<HDFS_destination>/jars/examples/etlflow-examples_2.12-0.7.19.jar,hdfs://<HDFS_destination>/jars/examples/etlflow-core-assembly-0.7.19.jar \
          run_job --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps  --props ratings_input_path=hdfs://<HDFS_destination>/examples/input,ratings_output_table_name=ratings,ratings_output_dataset=test,ratings_output_file_name=ratings.orc
 