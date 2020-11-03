---
layout: docs
title: Etlflow Core
---

## Etlflow Core

Etlflow Core is one of component of etlflow library which we can use to define the core functionality of etl jobs like etljobprops, etljobname and other utility functions. This component is the main building block of etljob library.

### STEP 1) Building an assembly jar for etlflow core
To build an assembly jar for an etlflow core library we need to perform some steps. Clone this git repo and go inside repo root folder and perform below steps: 
       
         
      > sbt
      > project core
      > assembly
      
* Inside root folder run 'sbt' command
* Inside sbt, Run 'project core' to set the current project as etlflow-core
* Once project gets set then run 'assembly' to build an assembly jar.       

To use etlflow cloud library in project add below setting in build.sbt file : 

```

lazy val etlflowCore = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#minimal"), "core")
lazy val d(project in file("modules/examples"))
  .dependsOn(etlflowCore)
         
```

### STEP 2) Copy the etlflow core assembly jar to gcs bucket using below command
 
    gsutil cp modules/core/target/scala-2.12/etlflow-core-assembly-x.x.x.jar gs://<BUCKET-NAME>/jars/examples
      
Replace x.x.x with the latest version [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.tharwaninitin/etlflow-core_2.12/badge.svg)](https://mvnrepository.com/artifact/com.github.tharwaninitin/etlflow-core)

## Example : 

Below is the step of core library using which we can run the query on postgres db table : 
   
### Define EtlJobProps
Here we can have any kind of logic for creating static or dynamic input parameters for job.
For e.g. intermediate path can be dynamically generated for every run based on current date.

```scala mdoc      
      
import etlflow.EtlJobProps
import java.text.SimpleDateFormat
import java.time.LocalDate
      
lazy val canonical_path = new java.io.File(".").getCanonicalPath
lazy val input_file_path = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
val date_prefix = LocalDate.now.toString.replace("-","")
      
case class EtlJob1Props (
   ratings_input_path: String = input_file_path,
   ratings_intermediate_bucket: String = sys.env("GCS_BUCKET"),
   ratings_intermediate_file_key: String = s"temp/$date_prefix/ratings.parquet",
   ratings_output_dataset: String = "test",
   ratings_output_table_name: String = "ratings",
) extends EtlJobProps
```

### Define the EtlJobName : 

Below is the example of etljobname where we can define the job name and its properties.

```

import etlflow.{EtlJobName, EtlJobProps}
import etlflow.etljobs.EtlJob
import examples.jobs._
import examples.schema.MyEtlJobProps._
sealed trait MyEtlJobName[+EJP <: EtlJobProps] extends EtlJobName[EJP]

object MyEtlJobName {
   case object Job1LocalJobDPSparkStep extends MyEtlJobName[EtlJob1Props] {
     override def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props()
     def etlJob(job_properties: Map[String, String]): EtlJob[EtlJob1Props] = EtlJob1(getActualProperties(job_properties))
   }
}


```

### Define the EtlJob  : 


```scala mdoc

import etlflow.etlsteps.{DPHiveJobStep, DPSparkJobStep}
import etlflow.utils.Executor.DATAPROC
import etlflow.etljobs.GenericEtlJob
import etlflow.EtlJobProps
import etlflow.etlsteps.DBQueryStep
import etlflow.utils._

case class EtlJob1(job_properties: EtlJob1Props) extends GenericEtlJob[EtlJob1Props] {

  val step = DBQueryStep(
     name  = "UpdatePG",
     query = "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;",
     credentials = JDBC("jdbc_url", "jdbc_user", "jdbc_pwd", "jdbc_driver")
  )
   
  val job = for {
      _ <- step.execute()
  } yield ()
}
```

### Define the Main components : 

Below is the 2 way we can execute the the etljobs by defining : 
* Loaddata

```

import etlflow.{EtlFlowApp, EtlJobProps}
import examples.schema.MyEtlJobName
   
object LoadData extends EtlFlowApp[MyEtlJobName[EtlJobProps], EtlJobProps]
```


To be able to use this core library, first you need postgres instance up and running, then create new database in pg (for e.g. etlflow), then set below environment variables.

```
export LOG_DB_URL=jdbc:postgresql://localhost:5432/etlflow
export LOG_DB_USER=<...>
export LOG_DB_PWD=<..>
export LOG_DB_DRIVER=org.postgresql.Driver

``` 

Now to create database tables used in this library run below commands from repo root folder:

```
sbt
project examples
runMain examples.LoadData run_db_migration

```

Now to run sample job use below command:

```
sbt
project examples
runMain examples.LoadData run_job --job_name Job2LocalJobGenericStep

```