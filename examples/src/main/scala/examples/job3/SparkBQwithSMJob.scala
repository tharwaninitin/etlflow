package examples.job3

import etljobs.etlsteps.{BQLoadStep, SparkReadWriteStep}
import etljobs.utils.{CSV, PARQUET, SessionManager, GlobalProperties, LOCAL, AppLogger}
import etljobs.functions.SparkUDF
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.Dataset

object RatingsSchemas {
  case class Rating( user_id:Int, movie_id: Int, rating : Double, timestamp: Long )
  case class RatingOutput( user_id:Int, movie_id: Int, rating : Double, timestamp: Long, date : java.sql.Date, date_int : String )
}

object SparkBQwithSMJob extends App {
  AppLogger.initialize
  val etl_job_logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.WARN)
  private val canonical_path : String = new java.io.File(".").getCanonicalPath
  private val global_properties = new GlobalProperties(s"$canonical_path/configurations/loaddata.properties" ) {}
  private val job_properties : Map[String,String] = Map(
    "ratings_input_path" -> s"$canonical_path/examples/src/main/resources/input/movies/ratings/*",
    "ratings_output_path" -> s"$canonical_path/examples/src/main/resources/output/movies/ratings",
    "ratings_output_dataset" -> "test",
    "ratings_output_table_name" -> "ratings",
    "ratings_output_file_name" -> "ratings.parquet"
  )
  val etljob = new SparkBQwithSMJob(job_properties, Some(global_properties))
}

class SparkBQwithSMJob(job_properties: Map[String,String], global_properties: Option[GlobalProperties]) extends SessionManager(global_properties) with SparkUDF {
  import RatingsSchemas._

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
  )(spark)

  val step2 = new BQLoadStep(
    name                = "LoadRatingBQ",
    source_path         = job_properties("ratings_output_path") + "/" + job_properties("ratings_output_file_name"),
    source_format       = PARQUET,
    source_file_system  = LOCAL,
    destination_dataset = job_properties("ratings_output_dataset"),
    destination_table   = job_properties("ratings_output_table_name")
  )(bq)

  SparkBQwithSMJob.etl_job_logger.info("##################JOB PROPERTIES##################")
  SparkBQwithSMJob.etl_job_logger.info(step1.name)
  step1.getStepProperties().foreach(x => SparkBQwithSMJob.etl_job_logger.info("==> " + x))
  SparkBQwithSMJob.etl_job_logger.info(step2.name)
  step2.getStepProperties().foreach(x => SparkBQwithSMJob.etl_job_logger.info("==> " + x))
  SparkBQwithSMJob.etl_job_logger.info("##################JOB PROPERTIES##################")

  // This below for comprehension is not lazy, it will run here only
  // Also it will stop after it encounters error in any step
  val opt = for {
    _ <- step1.process()
    _ <- step2.process()
  } yield ()

  // This is to throw exception in case of failure
  opt.get

  SparkBQwithSMJob.etl_job_logger.info("################RUN TIME PROPERTIES####################")
  SparkBQwithSMJob.etl_job_logger.info(step1.getExecutionMetrics.foreach(println))
  SparkBQwithSMJob.etl_job_logger.info(step2.getExecutionMetrics.foreach(println))
  SparkBQwithSMJob.etl_job_logger.info("################RUN TIME PROPERTIES####################")
  
  spark.stop()
}
