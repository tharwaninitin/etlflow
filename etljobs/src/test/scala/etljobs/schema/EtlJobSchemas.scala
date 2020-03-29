package etljobs.schema

object EtlJobSchemas {
  case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long)
  case class RatingOutput(user_id: Int, movie_id: Int, rating : Double, timestamp: Long, date: java.sql.Date)
  case class RatingBQ(user_id: BigInt, movie_id: BigInt, rating: Double)
}
