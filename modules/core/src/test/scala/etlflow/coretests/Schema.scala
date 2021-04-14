package etlflow.coretests

import etlflow.EtlJobProps
import etlflow.utils.{Executor, LoggingLevel}
import etlflow.utils.Executor.KUBERNETES
import io.circe.generic.semiauto.deriveDecoder

object Schema {

  case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long)
  case class RatingBQ(user_id: Long, movie_id: Long, rating: Double, timestamp: Long)
  case class RatingOutput(user_id: Int, movie_id: Int, rating: Double, timestamp: Long, date: java.sql.Date, date_int: Int)
  case class RatingOutputCsv(`User Id`: Int, `Movie Id`: Int, `Ratings`: Double, `Movie Date`: java.sql.Date)
  case class RatingsMetrics(sum_ratings: Double, count_ratings: Long)
  case class EtlJobRun(job_name: String, job_run_id:String, state:String)
  case class HttpBinResponse(
      args: Map[String, String],
      headers: Map[String, String],
      origin: String,
      url: String,
    )


  case class Student(id: String, name: String, `class`: Option[String])

  // circe decoder for HttpBinResponse
  object HttpBinResponse {
    implicit val httpBinResponseDecoder = deriveDecoder[HttpBinResponse]
  }

  // circe decoder for Student
  object Student {
    implicit val studentDecoder = deriveDecoder[Student]
  }

  private val canonical_path = new java.io.File(".").getCanonicalPath

  case class EtlJob1Props() extends EtlJobProps

  private val input_file_path = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  case class EtlJob2Props (
    ratings_input_path: List[String] = List(input_file_path),
    ratings_output_table_name: String = "ratings",
  ) extends EtlJobProps

  case class EtlJob23Props (
                             ratings_input_path: String = input_file_path,
                             ratings_output_dataset: String = "test",
                             ratings_output_table_name: String = "ratings"
                           ) extends EtlJobProps

  case class EtlJob3Props() extends EtlJobProps
  case class EtlJob4Props() extends EtlJobProps

  case class EtlJob5Props() extends EtlJobProps

  case class EtlJob6Props (
                            ratings_input_dataset: String = "test",
                            ratings_input_table_name: String = "ratings",
                            ratings_intermediate_bucket: String = s"gs://${sys.env("GCS_BUCKET")}/intermediate/ratings",
                            ratings_output_bucket_1: String = s"gs://${sys.env("GCS_BUCKET")}/output/ratings/csv",
                            ratings_output_bucket_2: String = s"s3a://${sys.env("S3_BUCKET")}/temp/output/ratings/parquet",
                            ratings_output_bucket_3: String = s"gs://${sys.env("GCS_BUCKET")}/output/ratings/json",
                            ratings_output_file_name: Option[String] = Some("ratings.csv"),
                          ) extends EtlJobProps
}
