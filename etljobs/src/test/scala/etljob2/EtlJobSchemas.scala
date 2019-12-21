package etljob2

object EtlJobSchemas {
  case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long)
  case class RatingOutput(user_id: Int, movie_id: Int, rating : Double, timestamp: Long, date: java.sql.Date, temp_date_col: String)
}
