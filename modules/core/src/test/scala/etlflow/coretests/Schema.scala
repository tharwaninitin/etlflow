package etlflow.coretests

object Schema {
  case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long)
  case class RatingBQ(user_id: Long, movie_id: Long, rating: Double, timestamp: Long)
  case class RatingOutput(user_id: Int, movie_id: Int, rating: Double, timestamp: Long, date: java.sql.Date, date_int: Int)
  case class RatingOutputCsv(`User Id`: Int, `Movie Id`: Int, `Ratings`: Double, `Movie Date`: java.sql.Date)
  case class RatingsMetrics(sum_ratings: Double, count_ratings: Long)
  case class HttpBinResponse(
      args: Map[String, String],
      headers: Map[String, String],
      origin: String,
      url: String,
    )
  case class Student(id: String, name: String, `class`: Option[String])
}
