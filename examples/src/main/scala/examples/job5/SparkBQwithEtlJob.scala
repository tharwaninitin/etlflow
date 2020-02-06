package examples.job5

// BigQquery Imports
import com.google.cloud.bigquery.JobInfo
import EtlJobSchemas.Rating
import etljobs.etlsteps.StateLessEtlStep
import etljobs.{EtlJobName, EtlProps}
import etljobs.utils.GlobalProperties
// ETLJOB library specific Imports
import etljobs.EtlJob
import etljobs.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
import etljobs.utils.{LOCAL, PARQUET, CSV}
// Job specific imports
import org.apache.log4j.{Level, Logger}

case object SparkBQwithEtlJob extends EtlJobName

class SparkBQwithEtlJob(
                         val job_name: EtlJobName = SparkBQwithEtlJob,
                         val job_properties: Either[Map[String,String], EtlProps],
                         val global_properties: Option[GlobalProperties] = None
                      ) extends EtlJob {
  Logger.getLogger("org").setLevel(Level.WARN)
  val job_props: Map[String,String] = job_properties match {
    case Left(value) => value
  }

  val step1 = SparkReadWriteStep[Rating, Rating](
    name                    = "LoadRatingsParquet",
    input_location          = Seq(job_props("ratings_input_path")),
    input_type              = CSV(),
    output_type             = PARQUET,
    output_location         = job_props("ratings_output_path"),
    output_filename         = Some(job_props("ratings_output_file_name"))
  )(spark)
  
  val step2 = BQLoadStep(
    name                = "LoadRatingBQ",
    source_path         = job_props("ratings_output_path") + "/" + job_props("ratings_output_file_name"),
    source_format       = PARQUET,
    source_file_system  = LOCAL,
    destination_dataset = job_props("ratings_output_dataset"),
    destination_table   = job_props("ratings_output_table_name"),
    create_disposition  = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )(bq)

  val etl_step_list: List[StateLessEtlStep] = List(step1)
}
