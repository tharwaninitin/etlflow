package examples.jobs

import com.google.cloud.bigquery.JobInfo
import etljobs.EtlStepList
import etljobs.etljob.SequentialEtlJob
import etljobs.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
import etljobs.utils.{ORC, PARQUET}
import examples.MyGlobalProperties
import examples.schema.MyEtlJobProps
import examples.schema.MyEtlJobProps.EtlJob1Props
import examples.schema.MyEtlJobSchema.RatingOutput

case class EtlJob1Definition(job_properties: MyEtlJobProps, global_properties: Option[MyGlobalProperties]) extends SequentialEtlJob {

  private val gcs_output_path = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings"
  private val job_props = job_properties.asInstanceOf[EtlJob1Props]

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
    input_location            = Left(gcs_output_path + "/" + job_props.ratings_output_file_name.get),
    input_type                = ORC,
    output_dataset            = job_props.ratings_output_dataset,
    output_table              = job_props.ratings_output_table_name,
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )

  val etl_step_list: List[EtlStep[_, _]] = EtlStepList(step1,step2)
}
