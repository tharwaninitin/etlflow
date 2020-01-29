package etljob1

// BigQquery Imports
import com.google.cloud.bigquery.{JobInfo}
// ETLJOB library specific Imports
import etljobs.EtlJob
import etljobs.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
import etljobs.utils.{ORC, PARQUET, GlobalProperties, LOCAL}
// Job specific imports
import EtlJobSchemas.RatingOutput
import org.apache.log4j.{Level, Logger}

class EtlJobDefinition(job_properties: Map[String,String], global_properties: Option[GlobalProperties]) extends EtlJob(job_properties, global_properties) {
  var output_date_paths : Seq[String] = Seq()
  Logger.getLogger("org").setLevel(Level.WARN)

  val step1 = SparkReadWriteStep[RatingOutput, RatingOutput](
    name                    = "LoadRatingsParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = PARQUET,
    output_type             = ORC,
    output_location         = job_properties("ratings_output_path"),
    output_filename         = Some(job_properties("ratings_output_file_name"))
  )(spark)
  
  val step2 = BQLoadStep(
    name                = "LoadRatingBQ",
    source_path         = job_properties("ratings_output_path") + "/" + job_properties("ratings_output_file_name"),
    source_format       = ORC,
    source_file_system  = LOCAL,
    destination_dataset = job_properties("ratings_output_dataset"),
    destination_table   = job_properties("ratings_output_table_name"),
    create_disposition  = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )(bq)

  def apply(): List[EtlStep[Unit,Unit]] = List(step1,step2)
}
