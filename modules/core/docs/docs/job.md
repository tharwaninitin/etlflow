---
layout: docs
title: Job
---

## Job

**Job is collection of steps.** Any case class can be converted to EtlJob just by extending **EtlJob** trait. This traits requires to implement three objects as shown below.
* override val job_properties: EtlJobProps = ???
* override val global_properties: Option[GlobalProperties] = ???
* override def etlJob(implicit resource: LoggerResource): Task[Unit] = ???

### EtlJobProps

      case class EtlJob1Props (
        ratings_input_path: List[String] = List(""),
        ratings_output_dataset: String = "",
        ratings_output_table_name: String = "",
        ratings_output_file_name: Option[String] = Some("ratings.orc")
      ) extends EtlJobProps

### EtlJob
Below is the example of Generic EtlJob which has two steps which can execute in any order defined by composing ZIO effects. 

    import etlflow.LoggerResource
    import etlflow.etljobs.EtlJob
    import com.google.cloud.bigquery.JobInfo
    import etlflow.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
    import etlflow.utils.{ORC, PARQUET}
    import examples.MyGlobalProperties
    import examples.schema.MyEtlJobProps
    import examples.schema.MyEtlJobProps.EtlJob1Props
    import examples.schema.MyEtlJobSchema.RatingOutput
    import zio.Task
    
    case class EtlJob1(job_properties: EtlJobProps, global_properties: Option[GlobalProperties]) extends EtlJob {
        
      val job_props = job_properties.asInstanceOf[EtlJob1Props]
    
      val step1 = SparkReadWriteStep[RatingOutput](
              name            = "LoadRatingsParquet",
              input_location  = job_props.ratings_input_path,
              input_type      = PARQUET,
              output_type     = ORC,
              output_location = gcs_output_path,
              output_filename = job_props.ratings_output_file_name
            )
          
      val step2 = BQLoadStep(
              name                      = "LoadRatingBQ",
              input_location            = Left("gs://gcs_bucket/temp/" + job_props.ratings_output_file_name.get),
              input_type                = ORC,
              output_dataset            = job_props.ratings_output_dataset,
              output_table              = job_props.ratings_output_table_name,
              output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
            )
    
      def etlJob(implicit resource: LoggerResource): Task[Unit] = for {
        - <- step1.execute(spark)
        _ <- step2.execute()
       } yield ()
    }
    
### SequentialEtlJob
Below is the example of SequentialEtlJob which has two steps which can execute sequentially.
 
     import com.google.cloud.bigquery.JobInfo
     import etlflow.EtlStepList
     import etlflow.etljobs.SequentialEtlJob
     import etlflow.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
     import etlflow.utils.{ORC, PARQUET}
     import examples.MyGlobalProperties
     import examples.schema.MyEtlJobProps
     import examples.schema.MyEtlJobProps.EtlJob1Props
     import examples.schema.MyEtlJobSchema.RatingOutput
     
     case class EtlJob1(job_properties: EtlJobProps, global_properties: Option[GlobalProperties]) extends SequentialEtlJob {
     
       val job_props = job_properties.asInstanceOf[EtlJob1Props]
     
       val step1 = SparkReadWriteStep[RatingOutput](
         name            = "LoadRatingsParquet",
         input_location  = job_props.ratings_input_path,
         input_type      = PARQUET,
         output_type     = ORC,
         output_location = gcs_output_path,
         output_filename = job_props.ratings_output_file_name
       )
     
       val step2 = BQLoadStep(
         name                      = "LoadRatingBQ",
         input_location            = Left("gs://gcs_bucket/temp/" + job_props.ratings_output_file_name.get),
         input_type                = ORC,
         output_dataset            = job_props.ratings_output_dataset,
         output_table              = job_props.ratings_output_table_name,
         output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
       )
     
       val etlStepList = EtlStepList(step1,step2)
     }