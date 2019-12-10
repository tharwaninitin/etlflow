package etljob1

import EtlJobSchemas.{Rating, RatingOutput}
import com.google.cloud.bigquery.BigQueryOptions
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Encoders, SparkSession}
import etljobs.EtlJob
import etljobs.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStateStep}
import etljobs.etlsteps.SparkReadWriteStateStep.{Input, Output}
import etljobs.functions.SparkUDF
import etljobs.utils.{CSV, PARQUET, Settings}
import org.apache.log4j.{Level, Logger}
import etljobs.utils.SessionManager

/**
* In first step it reads ratings data from ratings_input_path mentioned in input parameters
* then enrich it using function enrichRatingData and writes in PARQUET format at given output path
*
* In second step it reads PARQUET data stored by step1 and writes it to BigQuery table
*/

class EtlJobDefinition(val job_properties : Map[String,String]) extends EtlJob with SparkUDF with SessionManager {
  var output_date_paths : Seq[String] = Seq()
  Logger.getLogger("org").setLevel(Level.WARN)
  // lazy val spark  = SparkSession.builder().master("local[*]").getOrCreate()
  // lazy val bq = BigQueryOptions.getDefaultInstance.getService
  val canonical_path = new java.io.File(".").getCanonicalPath
  override lazy val settings =  new Settings(canonical_path + "/etljobs/src/test/resources/loaddata.properties")

  val step1 = SparkReadWriteStateStep[Rating , Unit, RatingOutput, Unit](
    name                    = "LoadRatingsParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(",", true, job_properties.getOrElse("parse_mode","FAILFAST")),
    transform_function      = Some(enrichRatingData(spark, job_properties)),
    output_type             = PARQUET,
    output_location         = job_properties("ratings_output_path"),
    output_filename         = Some(job_properties("ratings_output_file_name"))
  )(spark,job_properties)
  
  val step2 = new BQLoadStep(
    name                = "LoadRatingBQInsideFor",
    source_path         = job_properties("ratings_output_path") + "/" + job_properties("ratings_output_file_name"),
    destination_dataset = job_properties("ratings_output_dataset"),
    destination_table   = job_properties("ratings_output_table_name"),
    source_format       = PARQUET
  )(bq,job_properties)

  /**
  * Enriches ratings dataset by adding columns year, date, date_int
  * and casting column date to date type

  * @param spark spark session
  * @param job_properties map of key value containing input parameters
  * @param in raw dataset which needs to be enriched
  * @return ratings enriched dataframe
  */

  def enrichRatingData(spark: SparkSession, job_properties : Map[String, String])(in : Input[Rating, Unit]) : Output[RatingOutput,Unit] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in.ds
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn("date_int", get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))
        .withColumn("barc_year" , get_barc_year_udf(col("date")))

    val ratings_ds = ratings_df.as[RatingOutput](mapping)

    Output[RatingOutput,Unit](ratings_ds,())
  }

  def apply() : List[EtlStep[Unit,Unit]] = {
    val list = List(step1,step2)
    list
  }
}
