package object etljobs {
  trait EtlJobName
  trait EtlProps {
    val job_run_id: String
    val job_name: EtlJobName
    val job_description: String = ""
    val aggregate_error: Boolean = false
  }
  case class EtlJobException(msg : String) extends Exception
}
