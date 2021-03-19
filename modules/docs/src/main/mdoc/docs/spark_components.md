---
layout: docs
title: Etlflow Spark
---

## Etlflow Spark

To use etlflow Spark library in project add below setting in build.sbt file : 

```

    lazy val etlflowSpark = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#minimal"), "spark")
    lazy val docs = (project in file("modules/examples"))
        .dependsOn(etlflowSpark)
         
```
### Etlflow Spark library contains modules like :
* AWS : This module can get used when we want to read/write data to and from aws buckets. 
* GCP : This module can get used when we want to read/write data to and from gcp buckets.

### STEP 1) Building an assembly jar for etlflow Spark
To build an assembly jar for an etlflow spark library we need to perform some steps. Clone this git repo and go inside repo root folder and perform below steps: 
       
         
      > sbt
      > project spark
      > assembly
      
* Inside root folder run 'sbt' command
* Inside sbt, Run 'project spark' to set the current project as etlflow-spark
* Once project gets set then run 'assembly' to build an assembly jar.       

### STEP 2) Copy the etlflow spark assembly jar to gcs bucket using below command
 
      gsutil cp modules/spark/target/scala-2.12/etlflow-spark-assembly-x.x.x.jar gs://<BUCKET-NAME>/jars/examples
      
Replace x.x.x with the latest version [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-spark_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-spark)
    
## Example : 

Below is the step of spark library using which we can use the sparksession and transform the data : 

**Create spark session**   

```scala mdoc
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger}
LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
implicit lazy val spark: SparkSession  = SparkSession.builder().master("local[*]").getOrCreate()       

```
         
```scala mdoc
         
import etlflow.etlsteps._
import etlflow.utils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{Encoders, Dataset}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import etlflow.etljobs.GenericEtlJob
import etlflow.EtlJobProps
import etlflow.spark.IOType
import etlflow.gcp.BQInputType
         
case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long) extends EtlJobProps
         
case class EtlJob1(job_properties: Rating) extends GenericEtlJob[Rating] {

   lazy val step1 = SparkReadWriteStep[Rating](
        name             = "LoadRatingsParquetToJdbc",
        input_location   = Seq("gs://path/to/input/*"),
        input_type       = IOType.PARQUET,
        output_type      = IOType.JDBC("jdbc_url", "jdbc_user", "jdbc_pwd", "jdbc_driver"),
        output_location  = "ratings",
        output_save_mode = SaveMode.Overwrite
   )

   val job = for {
       _ <- step1.execute()
   } yield ()
}
```