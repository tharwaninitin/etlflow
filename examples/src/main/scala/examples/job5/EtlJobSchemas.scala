package examples.job5

object EtlJobSchemas {
  case class Rating( user_id:Int, movie_id: Int, rating : Double, timestamp: Long )
}
