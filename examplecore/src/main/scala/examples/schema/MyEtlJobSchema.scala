package examples.schema

sealed trait MyEtlJobSchema

object MyEtlJobSchema {
  case class EtlJobRun(job_name: String, job_run_id:String, state:String)
}
