---
layout: docs
title: Using with GCP Dataproc 
---

## Using Etlflow core library in your ETL projects on GCP Dataproc 

### Prerequisite
* Your system should have [Google Cloud Sdk](https://cloud.google.com/sdk/install) installed.
* You should have Dataproc cluster created.
* You should appropriate permissions for GCS, BigQuery, Submitting jobs on Dataproc etc

### STEP 1) Building an assembly jar for etlflow core
To build an assembly jar for an etlflow core library we need to perform some steps. Clone this git repo and go inside repo root folder and perform below steps: 
       
         
      > sbt
      > project core
      > assembly
      
* Inside root folder run 'sbt' command
* Inside sbt, Run 'project core' to set the current project as etlflow-core
* Once project gets set then run 'assembly' to build an assembly jar.       

### STEP 2) Copy the etlflow core assembly jar to gcs bucket using below command
 
      gsutil cp modules/core/target/scala-2.12/etlflow-core-assembly-x.x.x.jar gs://<BUCKET-NAME>/jars/examples
      
Replace x.x.x with the latest version [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

### Note
Sometime spark applications may fail if spark application dependencies conflict with Dataproc Hadoop's dependencies. This conflict can arise because Hadoop injects its dependencies into the application's classpath, so its dependencies take precedence over the application's dependencies. When a conflict occurs, NoSuchMethodError or other errors can be generated. 

One such library is Guava, which is the Google core library for Java that is used by many libraries and frameworks, including Hadoop. A dependency conflict can occur if a job or its dependencies require a version of Guava that is newer than the one used by Hadoop. To resolve such errors we can define below settings in build.sbt for assembly jar


      assemblyMergeStrategy in assembly := {
           case PathList("META-INF", xs @ _*) => MergeStrategy.discard
           case x => MergeStrategy.first
      },
      assemblyShadeRules in assembly := Seq(
           ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
      ),

For reference: [manage-spark-dependencies](https://cloud.google.com/dataproc/docs/guides/manage-spark-dependencies)     

### STEP 3) Building a jar for application, here we will take examples scala project in this library

* To build jar for an application perform below commands from project root folder: 
       
         
      > sbt
      > project examples
      > package
      
* From root folder run 'sbt' command
* Inside sbt, Run 'project examples' to set the current project as examples.
* Once project gets set then run 'package' to build an assembly jar.
       
### STEP 4) Copy the examples jar to gcs bucket using below command:
 
      gsutil cp examples/target/scala-2.12/etlflow-examples_2.12-x.x.x.jar  gs://<BUCKET-NAME>/jars/examples

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
  
* Now Copy updated application.conf file at gcs bucket using below command:      
        
        gsutil cp examples/src/main/resources/application.conf  gs://<BUCKET-NAME>/jars/examples
           
* Copy the input file present at location examples/src/main/data/movies/ratings_parquet/ratings.parquet for running sample job using below command:
    
        gsutil cp examples/src/main/data/movies/ratings_parquet/ratings.parquet  gs://<BUCKET-NAME>/examples/input

       
### STEP 6) Now finally we can run sample job on Dataproc using below command: 


      gcloud dataproc jobs submit spark \
         --project <project-name> \
         --region <region-name> \
         --cluster <cluster-name> \
         --class examples.LoadData \
         --jars gs://<BUCKET-NAME>/jars/examples/etlflow-examples_2.12-0.7.19.jar,gs://<BUCKET-NAME>/jars/examples/etlflow-core-assembly-0.7.19.jar \
         -- run_job --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps  --props ratings_input_path=gs://<BUCKET-NAME>/examples/input,ratings_output_table_name=ratings,ratings_output_dataset=test,ratings_output_file_name=ratings.orc
        
  
 