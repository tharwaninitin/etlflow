package examples.schema

sealed trait MyEtlJobSchema

object MyEtlJobSchema {
  case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long) extends MyEtlJobSchema
  case class RatingOutput(user_id: Int, movie_id: Int, rating : Double, timestamp: Long, date: java.sql.Date, temp_date_col: String) extends MyEtlJobSchema
  case class RatingBQ(userId: BigInt, movieId: BigInt, rating: Double, timestamp: Long) extends MyEtlJobSchema
  case class EtlJobRun(job_name: String, job_run_id:String, state:String)

}
