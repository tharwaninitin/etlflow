package etljob1

// BigQquery Imports
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.{JobInfo}
// Spark Imports
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Encoders, SparkSession, Dataset}
// ETLJOB library specific Imports
import etljobs.EtlJob
import etljobs.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
import etljobs.utils.{CSV, ORC, Settings}
import etljobs.utils.SessionManager
// Job specific imports
import EtlJobSchemas.{Rating, RatingOutput}
import org.apache.log4j.{Level, Logger}


class EtlJobDefinition(val job_properties : Map[String,String]) extends EtlJob {
  var output_date_paths : Seq[String] = Seq()
  Logger.getLogger("org").setLevel(Level.WARN)

  // Overriding Settings object to take local loaddata.properties
  // Only required while testing on local, in production this file should be provided to spark using spark-submit
  override lazy val settings =  new Settings(new java.io.File(".").getCanonicalPath + "/etljobs/src/test/resources/loaddata.properties")

  def enrichRatingData(spark: SparkSession, job_properties : Map[String, String])(in : Dataset[Rating]) : Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))

    ratings_df.as[RatingOutput](mapping)
  }

  val step1 = SparkReadWriteStep[Rating, RatingOutput](
    name                    = "LoadRatingsParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(",", true, job_properties.getOrElse("parse_mode","FAILFAST")),
    transform_function      = Some(enrichRatingData(spark, job_properties)),
    output_type             = ORC,
    output_location         = job_properties("ratings_output_path"),
    output_filename         = Some(job_properties("ratings_output_file_name"))
  )(spark,job_properties)
  
  val step2 = BQLoadStep(
    name                = "LoadRatingBQ",
    source_path         = job_properties("ratings_output_path") + "/" + job_properties("ratings_output_file_name"),
    destination_dataset = job_properties("ratings_output_dataset"),
    destination_table   = job_properties("ratings_output_table_name"),
    source_format       = ORC,
    create_disposition  = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )(bq,job_properties)

  def apply() : List[EtlStep[Unit,Unit]] = List(step1,step2)
}
