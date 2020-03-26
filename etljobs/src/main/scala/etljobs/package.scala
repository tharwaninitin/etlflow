package object etljobs {
  trait EtlJobName
  trait EtlJobProps {
    val job_name: EtlJobName
    val job_properties: Map[String,String]
    val job_run_id: String                    = java.util.UUID.randomUUID.toString
    var job_description: String               = job_properties.getOrElse("job_description", "")
    var job_aggregate_error: Boolean          = job_properties.getOrElse("job_aggregate_error", "false").toBoolean
    var job_send_slack_notification: Boolean  = job_properties.getOrElse("job_send_slack_notification", "false").toBoolean
    var job_notification_level: String        = job_properties.getOrElse("job_notification_level", "debug") //info or debug
  }
  case class EtlJobException(msg : String) extends Exception
}
