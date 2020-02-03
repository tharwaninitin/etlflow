package object etljobs {
  trait EtlJobName
  trait EtlProps {
    val job_run_id: String
    val job_name: EtlJobName
  }
  case class EtlJobException(msg : String) extends Exception
}
