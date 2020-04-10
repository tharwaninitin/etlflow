package etljobs.examples.etljob1

// BigQuery Imports
import com.google.cloud.bigquery.JobInfo
import etljobs.bigquery.BigQueryManager
import etljobs.etlsteps.StateLessEtlStep
import etljobs.examples.MyGlobalProperties
import etljobs.examples.schema.MyEtlJobSchema.RatingOutput
import etljobs.examples.schema.MyEtlJobProps.EtlJob1Props
import etljobs.spark.SparkManager
import etljobs.EtlStepList
import etljobs.examples.schema.MyEtlJobProps
// ETLJOB library specific Imports
import etljobs.EtlJob
import etljobs.etlsteps.{BQLoadStep, SparkReadWriteStep}
import etljobs.utils.{ORC, PARQUET}

case class EtlJobDefinition(job_properties: MyEtlJobProps, global_properties: Option[MyGlobalProperties]) extends EtlJob with SparkManager with BigQueryManager {

  private val gcs_output_path = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings"
  private val job_props:EtlJob1Props = job_properties.asInstanceOf[EtlJob1Props]

  val step1 = SparkReadWriteStep[RatingOutput](
    name            = "LoadRatingsParquet",
    input_location  = job_props.ratings_input_path,
    input_type      = PARQUET,
    output_type     = ORC,
    output_location = gcs_output_path,
    output_filename = job_props.ratings_output_file_name
  )(spark)

  val step2 = BQLoadStep(
    name                      = "LoadRatingBQ",
    input_location            = Left(gcs_output_path + "/" + job_props.ratings_output_file_name.get),
    input_type                = ORC,
    output_dataset            = job_props.ratings_output_dataset,
    output_table              = job_props.ratings_output_table_name,
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )(bq)

  val etl_step_list:List[StateLessEtlStep] = EtlStepList(step1,step2)
}
