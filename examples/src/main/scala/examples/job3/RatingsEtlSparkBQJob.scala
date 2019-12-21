package examples.job3

import etljobs.etlsteps.{BQLoadStep, SparkReadWriteStep}
import etljobs.utils.{CSV, PARQUET, SessionManager, Settings}
import etljobs.functions.SparkUDF
import examples.job3.RatingsSchemas.{Rating, RatingOutput}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.Dataset

/**
* This example contains two steps and uses SessionManager for SparkSession and Bigquery
* In first step it reads ratings data from ratings_input_path mentioned in input parameters
* then enrich it using function enrichRatingData and writes in PARQUET format at given output path
*
* In second step it reads PARQUET data stored by step1 and writes it to BigQuery table
*/

object RatingsEtlSparkBQJob extends App with SparkUDF with SessionManager {
  // Use this logger level to change log information
  Logger.getLogger("org").setLevel(Level.WARN)

  private val canonical_path : String = new java.io.File(".").getCanonicalPath
  private val etl_job_logger = Logger.getLogger(getClass.getName)
  override lazy implicit val settings: Settings = new Settings(s"$canonical_path/configurations/loaddata.properties")

  val job_properties : Map[String,String] = Map(
    "ratings_input_path" -> s"$canonical_path/examples/src/main/resources/input/movies/ratings/*",
    "ratings_output_path" -> s"$canonical_path/examples/src/main/resources/output/movies/ratings",
    "ratings_output_dataset" -> "test",
    "ratings_output_table_name" -> "ratings",
    "ratings_output_file_name" -> "ratings.parquet",
    //"parse_mode" -> "PERMISSIVE",
    "test"-> "true"
  )

  /**
    * RatingsEtlJob Enriches ratings dataset by adding columns year, date, date_int
    * and casting column date to date type

    * @param spark spark session
    * @param job_properties map of key value containing input parameters
    * @param in raw dataset which needs to be enriched with input state
    * @return ratings enriched dataset with output state
  */
  def enrichRatingData(spark: SparkSession, job_properties : Map[String, String])(in : Dataset[Rating]) : Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn("date_int", get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))

    val ratings_ds = ratings_df.as[RatingOutput](mapping)

    ratings_ds
  }

  val step1 = new SparkReadWriteStep[Rating, RatingOutput](
    name                    = "LoadRatingsParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(",", true, job_properties.getOrElse("parse_mode","FAILFAST")),
    transform_function      = Some(enrichRatingData(spark, job_properties)),
    output_type             = PARQUET,
    output_location         = job_properties("ratings_output_path"),
    output_filename         = Some(job_properties("ratings_output_file_name"))
  )(spark,job_properties)

  val step2 = new BQLoadStep(
    name                = "LoadRatingBQ",
    source_path         = job_properties("ratings_output_path") + "/" + job_properties("ratings_output_file_name"),
    destination_dataset = job_properties("ratings_output_dataset"),
    destination_table   = job_properties("ratings_output_table_name"),
    source_format       = PARQUET
  )(bq,job_properties)

  println("##################################JOB PROPERTIES########################################")
  println(step1.name + " : " + step1.getStepProperties.foreach(println))
  println(step2.name + " : " + step2.getStepProperties.foreach(println))
  println("##################################JOB PROPERTIES########################################")

  // This below for comprehension is not lazy, it will run here only
  // Also it will stop after it encounters error in any step
  val opt = for {
    _ <- step1.process()
    _ <- step2.process()
  } yield ()

  // This is to throw exception in case of failure
  opt.get

  println("##################################RUN TIME PROPERTIES########################################")
  println(step1.getExecutionMetrics.foreach(println))
  println(step2.getExecutionMetrics.foreach(println))
  println("##################################RUN TIME PROPERTIES########################################")
  
  spark.stop()
}
