package examples.job2

import org.apache.spark.sql.{SparkSession}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import etljobs.etlsteps.{SparkReadWriteStep, BQLoadStep}
import etljobs.utils.{CSV,PARQUET,LOCAL}
import org.apache.log4j.{Level, Logger}

object SparkBQJob extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  
  private lazy val spark : SparkSession  = SparkSession.builder().master("local[*]").getOrCreate()
  private lazy val bq : BigQuery  = BigQueryOptions.getDefaultInstance.getService
  private val canonical_path : String = new java.io.File(".").getCanonicalPath

  val job_properties : Map[String,String] = Map(
    "ratings_input_path" -> s"$canonical_path/examples/src/main/resources/input/movies/ratings/*",
    "ratings_output_path" -> s"$canonical_path/examples/src/main/resources/output/movies/ratings",
    "ratings_output_dataset" -> "test",
    "ratings_output_table_name" -> "ratings",
    "ratings_output_file_name" -> "ratings.parquet"
  )

  case class Rating( user_id:Int, movie_id: Int, rating : Double, timestamp: Long )

  val step1 = new SparkReadWriteStep[Rating, Rating](
    name                    = "ConvertRatingsCSVtoParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(",", true, job_properties.getOrElse("parse_mode","FAILFAST")),
    output_location         = job_properties("ratings_output_path"),
    output_type             = PARQUET,
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

  val opt = for {
    _ <- step1.process()
    _ <- step2.process()
  } yield ()

  opt.get

  spark.stop
}