package examples.schema

import etlflow.EtlJobSchema

sealed trait MyEtlJobSchema extends EtlJobSchema

object MyEtlJobSchema {
  case class Rating(userid: Long, movieid: Long, rating: Double, timestamp: Long) extends MyEtlJobSchema
  case class RatingOutput(user_id: Int, movie_id: Int, rating : Double, timestamp: Long, date: java.sql.Date) extends MyEtlJobSchema
  case class RatingBQ(user_id: BigInt, movie_id: BigInt, rating: Double) extends MyEtlJobSchema
}
