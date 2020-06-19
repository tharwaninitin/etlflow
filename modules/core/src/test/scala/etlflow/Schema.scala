package etlflow

object Schema {
  case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long)
  case class RatingOutput(user_id: Int, movie_id: Int, rating: Double, timestamp: Long, date: java.sql.Date, date_int: Int)
  case class RatingOutputCsv(`User Id`: Int, `Movie Id`: Int, `Ratings`: Double, `Movie Date`: java.sql.Date)
  case class RatingsMetrics(sum_ratings: Double, count_ratings: Long)

  private val canonical_path = new java.io.File(".").getCanonicalPath
  private val input_file_path = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"

  case class EtlJob1Props (
    ratings_input_path: List[String] = List(input_file_path),
    ratings_intermediate_bucket: String = s"gs://${sys.env("GCS_BUCKET")}/output/ratings",
    ratings_output_dataset: String = "test",
    ratings_output_table_name: String = "ratings",
    ratings_output_file_name: Option[String] = Some("ratings.orc"),
  ) extends EtlJobProps
}

