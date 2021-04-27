---
layout: docs
title: Etlflow Core
---

## Quickstart (Etlflow Core)

### STEP 1) Define build.sbt: 
To use etlflow core library in project add below setting in build.sbt file

```

lazy val root = (project in file("."))
  .settings(
    scalaVersion := "2.12.13",
    libraryDependencies ++= List("com.github.tharwaninitin" %% "etlflow-core" % "x.x.x"))

```

### STEP 2) Define job properties EtlJobProps.scala:
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

### STEP 3) Define job EtlJob1.scala: 

```scala mdoc

import etlflow.etlsteps.{DPHiveJobStep, DPSparkJobStep}
import etlflow.utils.Executor.DATAPROC
import etlflow.EtlJobProps
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.DBQueryStep
import etlflow.utils._
import etlflow.Credential.JDBC

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

### STEP 4) Define job and properties mapping using EtlJobPropsMapping MyEtlJobPropsMapping.scala: 

Below is the example of defining EtlJobPropsMapping where we can define the mapping between job and its properties.

```scala mdoc

import etlflow.{EtlJobPropsMapping, EtlJobProps}
import etlflow.etljobs.EtlJob

sealed trait MyEtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]] extends EtlJobPropsMapping[EJP,EJ]

object MyEtlJobPropsMapping {
   case object Job1 extends MyEtlJobPropsMapping[EtlJob1Props,EtlJob1] {
     override def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props()
   }
}

```

### STEP 5) Define Main Runnable App LoadData.scala: 

```scala mdoc

import etlflow.{EtlFlowApp, EtlJobProps}
   
object LoadData extends EtlFlowApp[MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]]
```

### STEP 6) Running jobs: 
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
runMain LoadData run_db_migration

```

Now to run sample job use below command:

```
sbt
runMain LoadData run_job --job_name Job1

```