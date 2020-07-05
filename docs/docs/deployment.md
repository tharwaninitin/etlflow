---
layout: docs
title: Core Installation
---

## How to use Etlflow core library

**Building an assembly jar for etlflow core**
* To build an assembly jar for an etlflow core library we need to perform some steps. Clone this git repo and go inside repo root folder and perform below commands : 
       
         
      > sbt
      > project core
      > assembly
      
* From root folder run 'sbt' command
* Inside sbt, Run 'project core' to set the current project as etlflow-core
* Once project gets set then run 'assembly' to build an assembly jar.       
* Copy the etlflow core jar to gcs bucket using below command :
 
 
      gsutil cp modules/core/target/scala-2.12/etlflow-core-assembly-0.7.16.jar gs://<BUCKET-NAME>/jars/examples
      
**Building a jar for example library**

1.To build  jar for an etljobs ingestion library perform below commands from project root folder: 
       
         
      > sbt
      > project examples
      > package
      
* From root folder run 'sbt' command
* Inside sbt, Run 'project examples' to set the current project as examples.
* Once project gets set then run 'package' to build an assembly jar.
       
2.Copy the etljobs example jar to gcs bucket using below command :
 
 
      gsutil cp examples/target/scala-2.12/etlflow-examples_2.12-0.7.16.jar  gs://<BUCKET-NAME>/jars/examples
      

3.**examples/src/main/conf/loaddata.properties** folder contains the loaddata.properties file. Update the file with below content : 


        running_environment = gcp
        gcs_output_bucket = <BUCKET-NAME>
        log_db_url=<log_db_url>
        log_db_user=<log_db_user>
        log_db_pwd=<log_db_pwd>
        log_db_driver=org.postgresql.Driver
  
4.Copy updated loaddata file at gcs bucket using below command : 
        
        
        gsutil cp examples/src/main/conf/loaddata.properties  gs://<BUCKET-NAME>/jars/examples
           
5.Copy the input file present at location examples/src/main/data/movies/ratings_parquet/ratings.parquet using below command :
    
        gsutil cp examples/src/main/data/movies/ratings_parquet/ratings.parquet  gs://<BUCKET-NAME>/examples/input


           
* Command to run etljob : 


      gcloud dataproc jobs submit spark \
         --project <project-name> \
         --region <region-name> \
         --cluster <cluster-name> \
         --class examples.LoadData \
         --jars gs://<BUCKET-NAME>/jars/examples/etlflow-examples_2.12-0.7.16.jar,gs://<BUCKET-NAME>/jars/examples/etlflow-core-assembly-0.7.16.jar \
         --files gs://<BUCKET-NAME>/jars/examples/loaddata.properties \
         -- run_job --job_name EtlJob1PARQUETtoORCtoBQLocalWith2Steps  --props ratings_input_path=gs://<BUCKET-NAME>/examples/input,ratings_output_table_name=ratings,ratings_output_dataset=test,ratings_output_file_name=ratings.orc
        
  
## Note
  
* Below is the settings required in build.sbt to create assembly jar : 


      mainClass in assembly := Some("examples.Loaddata"),
      assemblyMergeStrategy in assembly := {
           case PathList("META-INF", xs @ _*) => MergeStrategy.discard
           case x => MergeStrategy.first
      },
      assemblyShadeRules in assembly := Seq(
           ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
      ),
     
Sometime spark applications may fail if spark application dependencies conflict with Hadoop's dependencies. This conflict can arise because Hadoop injects its dependencies into the application's classpath, so its dependencies take precedence over the application's dependencies. When a conflict occurs, NoSuchMethodError or other errors can be generated.

**Example:**

Guava is the Google core library for Java that is used by many libraries and frameworks, including Hadoop. A dependency conflict can occur if a job or its dependencies require a version of Guava that is newer than the one used by Hadoop.

Following two-part workaround to avoid possible dependency conflicts :

* Create a single "uber" JAR (aka "fat" JAR), that contains the application's package and all of its dependencies.
* Relocate the conflicting dependency packages within the uber JAR to prevent their path names from conflicting with those of Hadoop's dependency packages. Instead of modifying your code, use a plugin (see below) to automatically perform this relocation (aka "shading") as part of the packaging process.
The following is a sample  buid.sbt configuration file that shades the Guava library, which is located in the com.google.common package. This configuration instructs sbt to rename the com.google.common package to repackaged.com.google.common and to update all references to the classes from the original package. 
  
        
        assemblyShadeRules in assembly := Seq(
                  ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
        )
        
        
For reference : [manage-spark-dependencies](https://cloud.google.com/dataproc/docs/guides/manage-spark-dependencies)     