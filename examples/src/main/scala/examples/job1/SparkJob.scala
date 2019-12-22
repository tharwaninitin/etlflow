package examples.job1

import org.apache.spark.sql.{SparkSession}
import etljobs.etlsteps.{SparkReadWriteStep}
import etljobs.utils.{CSV,PARQUET}
import org.apache.log4j.{Level, Logger}

object SparkJob extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  
  private lazy val spark : SparkSession  = SparkSession.builder().master("local[*]").getOrCreate()
  private val canonical_path : String = new java.io.File(".").getCanonicalPath

  val job_properties : Map[String,String] = Map(
    "ratings_input_path" -> s"$canonical_path/examples/src/main/resources/input/movies/ratings/*",
    "ratings_output_path" -> s"$canonical_path/examples/src/main/resources/output/movies/ratings"
  )

  case class Rating( user_id:Int, movie_id: Int, rating : Double, timestamp: Long )

  val step1 = new SparkReadWriteStep[Rating, Rating](
    name                    = "ConvertRatingsCSVtoParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(),
    output_location         = job_properties("ratings_output_path"),
    output_type             = PARQUET
  )(spark,job_properties)

  println("##################################JOB PROPERTIES########################################")
  println(step1.name)
  step1.getStepProperties.foreach(println)
  println("##################################JOB PROPERTIES########################################")

  val try_output = step1.process()

  try_output.get

  spark.stop
}