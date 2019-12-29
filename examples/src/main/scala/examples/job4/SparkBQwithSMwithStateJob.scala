package examples.job4

import etljobs.etlsteps.{BQLoadStep, SparkReadWriteStateStep}
import etljobs.etlsteps.SparkReadWriteStateStep.{Input, Output}
import etljobs.utils.{CSV, PARQUET, SessionManager, GlobalProperties, LOCAL, AppLogger}
import etljobs.functions.SparkUDF
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object RatingsSchemas {
  case class Rating( user_id:Int, movie_id: Int, rating : Double, timestamp: Long )
  case class RatingOutput( user_id:Int, movie_id: Int, rating : Double, timestamp: Long, date : java.sql.Date, date_int : String )
}

object SparkBQwithSMwithStateJob extends App {
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
  val etljob = new SparkBQwithSMwithStateJob(job_properties, global_properties)
}

class SparkBQwithSMwithStateJob(job_properties: Map[String,String], global_properties: GlobalProperties) extends SessionManager(global_properties) with SparkUDF {
  import RatingsSchemas._
  import SparkBQwithSMwithStateJob.etl_job_logger
  
  def enrichRatingData(spark: SparkSession, job_properties : Map[String, String])(in : Input[Rating,Int]) : Output[RatingOutput,Int] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in.ds
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn("date_int", get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))

    val ratings_ds = ratings_df.as[RatingOutput](mapping)

    etl_job_logger.info(s"Input State is ${in.ips}")
    etl_job_logger.info(s"Output State is ${3}")

    Output[RatingOutput,Int](ratings_ds,3)
  }

  val step1 = new SparkReadWriteStateStep[Rating , Int, RatingOutput, Int](
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
    source_format       = PARQUET,
    source_file_system  = LOCAL,
    destination_dataset = job_properties("ratings_output_dataset"),
    destination_table   = job_properties("ratings_output_table_name")
  )(bq,job_properties)

  //    step1.map(enrichRatingData(spark, job_properties))
  //    for (x <- step1) {
  //      println(x)
  //    }
  //     val output = step1.map(x => x + 1)
  //     println("Output for map is " + output)

  etl_job_logger.info("##################################JOB PROPERTIES########################################")
  etl_job_logger.info(step1.name + " : " + step1.getStepProperties.foreach(etl_job_logger.info))
  etl_job_logger.info(step2.name + " : " + step2.getStepProperties.foreach(etl_job_logger.info))
  etl_job_logger.info("##################################JOB PROPERTIES########################################")

  // This below for comprehension is not lazy, it will run here only
  // Also it will stop after it encounters error in any step
  val opt = for {
    x <- step1.process(20)
    _ <- step2.process()
  } yield x

  // This is to throw exception in case of failure
  opt.get

  etl_job_logger.info("Output for for is " + opt)

  etl_job_logger.info("##################################RUN TIME PROPERTIES########################################")
  etl_job_logger.info(step1.getExecutionMetrics.foreach(etl_job_logger.info))
  etl_job_logger.info(step2.getExecutionMetrics.foreach(etl_job_logger.info))
  etl_job_logger.info("##################################RUN TIME PROPERTIES########################################")
  
  spark.stop()
}
