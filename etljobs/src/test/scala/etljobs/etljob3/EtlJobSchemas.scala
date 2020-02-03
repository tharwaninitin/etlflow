package etljobs.etljob3

import etljobs.{EtlJobName, EtlProps}

object EtlJobSchemas {
  case class EtlJob3Props (
      job_run_id: String,
      job_name: EtlJobName,
      ratings_input_path: String,
      ratings_output_path: String,
      ratings_output_dataset: String,
      ratings_output_table_name: String
    ) extends EtlProps
  case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long)
  case class RatingOutput(user_id: Int, movie_id: Int, rating : Double, timestamp: Long, date: java.sql.Date)
}
