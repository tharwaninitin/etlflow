---
layout: docs
title: Job
---

## Job

**Job is collection of steps.** Any case class can be converted to EtlJob just by extending **GenericEtlJob** trait. This traits requires to implement three objects as shown below.
* override val job_properties: EtlJobProps = ???
* override val globalProperties: Config = ???
* override val job: Task[Unit] = ???

Set these environment variables

    export GOOGLE_APPLICATION_CREDENTIALS=<...> # This should be full path to GCP Service Account Key Json which should have GCS and BigQuery Read/Write access
    export GCS_BUCKET=<...> # This is the intermediate GCS bucket where data will be uploaded for this example
    
Clone this git repo and go inside repo root folder and enter below command (make sure you have sbt and scala installed)

    SBT_OPTS="-Xms512M -Xmx1024M -Xss2M -XX:MaxMetaspaceSize=1024M" sbt -v "project examples" console

### Create EtlJobProps
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
### GenericEtlJob
Below is the example of GenericEtlJob which has two steps which can execute in any order defined by composing ZIO effects. 

```scala mdoc      
 
import com.google.cloud.bigquery.JobInfo
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.{BQLoadStep, GCSPutStep}
import etlflow.utils.PARQUET
import zio.Task
import etlflow.utils.Config

    
case class RatingOutput(user_id: Int, movie_id: Int, rating : Double, timestamp: Long, date: java.sql.Date)
    
case class EtlJob1(job_properties: EtlJob1Props) extends GenericEtlJob[EtlJob1Props] {
      
  val step1 = GCSPutStep(
          name    = "LoadRatingGCS",
          bucket  = job_properties.ratings_intermediate_bucket,
          key     = job_properties.ratings_intermediate_file_key,
          file    = job_properties.ratings_input_path
        )
          
  val step2 = BQLoadStep(
      name                      = "LoadRatingBQ",
      input_location            = Left(s"gs://${job_properties.ratings_intermediate_bucket}/${job_properties.ratings_intermediate_file_key}"),
      input_type                = PARQUET,
      output_dataset            = job_properties.ratings_output_dataset,
      output_table              = job_properties.ratings_output_table_name,
      output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )
    
  val job = for {
    _ <- step1.execute()
    _ <- step2.execute()
  } yield ()
}
```    

### SequentialEtlJob
Below is the example of SequentialEtlJob which is much simpler way to run jobs when all steps are needed to be run sequentially.

```scala mdoc      
 
import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps._
import etlflow.utils.Config
import etlflow.etlsteps.{BQLoadStep, GCSPutStep}
import etlflow.utils.PARQUET
import com.google.cloud.bigquery.JobInfo


 case class EtlJob2(job_properties: EtlJob1Props) extends SequentialEtlJob[EtlJob1Props] {

 val step3 = GCSPutStep(
                 name    = "LoadRatingGCS",
                 bucket  = job_properties.ratings_intermediate_bucket,
                 key     = job_properties.ratings_intermediate_file_key,
                 file    = job_properties.ratings_input_path
               )
                 
 val step4 = BQLoadStep(
             name                      = "LoadRatingBQ",
             input_location            = Left(s"gs://${job_properties.ratings_intermediate_bucket}/${job_properties.ratings_intermediate_file_key}"),
             input_type                = PARQUET,
             output_dataset            = job_properties.ratings_output_dataset,
             output_table              = job_properties.ratings_output_table_name,
             output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
         )
     
 val etlStepList = EtlStepList(step3,step4)
}
```