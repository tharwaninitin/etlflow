package examples.schema

sealed trait MyEtlJobSchema

object MyEtlJobSchema {
  case class Rating(user_id: Int, movie_id: Int, rating: Double, timestamp: Long) extends MyEtlJobSchema
  case class RatingBQ(user_id: BigInt, movie_id: BigInt, rating: Double)          extends MyEtlJobSchema
  case class EtlJobRun(job_name: String, job_run_id: String, state: String)
}
