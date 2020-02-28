package etljobs.etljob1

// BigQuery Imports
import com.google.cloud.bigquery.JobInfo
import etljobs.bigquery.BigQueryManager
import etljobs.schema.EtlJobList
import etljobs.schema.EtlJobSchemas.RatingOutput
import etljobs.spark.SparkManager
import etljobs.{EtlJobName, EtlProps}
// ETLJOB library specific Imports
import etljobs.EtlJob
import etljobs.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
import etljobs.utils.{ORC, PARQUET, GlobalProperties, LOCAL}
// Job specific imports
import org.apache.log4j.{Level, Logger}

class EtlJobDefinition(
                        val job_name: EtlJobName = EtlJobList.EtlJob1PARQUETtoORCtoBQLocalWith2StepsWithSlack,
                        val job_properties: Either[Map[String,String], EtlProps],
                        val global_properties: Option[GlobalProperties] = None
                      )
  extends EtlJob with SparkManager with BigQueryManager {
  var output_date_paths : Seq[String] = Seq()
  Logger.getLogger("org").setLevel(Level.WARN)
  val job_props: Map[String,String] = job_properties match {
    case Left(value) => value
  }

  val step1 = SparkReadWriteStep[RatingOutput, RatingOutput](
    name                    = "LoadRatingsParquet",
    input_location          = Seq(job_props("ratings_input_path")),
    input_type              = PARQUET,
    output_type             = ORC,
    output_location         = job_props("ratings_output_path"),
    output_filename         = Some(job_props("ratings_output_file_name"))
  )(spark)
  
  val step2 = BQLoadStep(
    name                = "LoadRatingBQ",
    source_path         = job_props("ratings_output_path") + "/" + job_props("ratings_output_file_name"),
    source_format       = ORC,
    source_file_system  = LOCAL,
    destination_dataset = job_props("ratings_output_dataset"),
    destination_table   = job_props("ratings_output_table_name"),
    create_disposition  = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )(bq)

  val etl_step_list:List[EtlStep[Unit,Unit]] = List(step1,step2)
}
