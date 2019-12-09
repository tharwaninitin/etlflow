package examples.job3

object RatingsSchemas {
  case class Rating( user_id:Int, movie_id: Int, rating : Double, timestamp: Long )
  case class RatingOutput( user_id:Int, movie_id: Int, rating : Double, timestamp: Long, date : java.sql.Date, date_int : String )
}
