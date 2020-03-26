package etljobs.etljob1

// BigQuery Imports
import com.google.cloud.bigquery.JobInfo
import etljobs.bigquery.BigQueryManager
import etljobs.schema.EtlJobProps.EtlJob1Props
import etljobs.schema.EtlJobSchemas.RatingOutput
import etljobs.spark.SparkManager
import etljobs.EtlJobProps
// ETLJOB library specific Imports
import etljobs.EtlJob
import etljobs.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
import etljobs.utils.{ORC, PARQUET, GlobalProperties, LOCAL}
// Job specific imports
import org.apache.log4j.{Level, Logger}

case class EtlJobDefinition(
                        job_properties: EtlJobProps,
                        global_properties: Option[GlobalProperties] = None
                      )
  extends EtlJob with SparkManager with BigQueryManager {
  var output_date_paths : Seq[String] = Seq()
  Logger.getLogger("org").setLevel(Level.WARN)

  val job_props:EtlJob1Props  = job_properties.asInstanceOf[EtlJob1Props]

  val step1 = SparkReadWriteStep[RatingOutput, RatingOutput](
    name                    = "LoadRatingsParquet",
    input_location          = job_props.ratings_input_path,
    input_type              = PARQUET,
    output_type             = ORC,
    output_location         = job_props.ratings_output_path,
    output_filename         = job_props.ratings_output_file_name
  )(spark)
  
  val step2 = BQLoadStep(
    name                              = "LoadRatingBQ",
    input_location                    = Left(job_props.ratings_output_path + "/" + job_props.ratings_output_file_name),
    input_type                        = ORC,
    input_file_system                 = LOCAL,
    output_dataset                    = job_props.ratings_output_dataset,
    output_table                      = job_props.ratings_output_table_name,
    output_table_create_disposition   = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )(bq)

  val etl_step_list:List[EtlStep[Unit,Unit]] = List(step1,step2)
}
