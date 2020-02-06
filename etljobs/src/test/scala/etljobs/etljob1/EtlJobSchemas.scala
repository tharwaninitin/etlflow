package etljobs.etljob1

object EtlJobSchemas {
  case class RatingOutput( user_id:Int, movie_id: Int, rating : Double, timestamp: Long, date : java.sql.Date)
}
