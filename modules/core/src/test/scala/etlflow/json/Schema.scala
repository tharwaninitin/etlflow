package etlflow.json

object Schema {
  sealed trait LoggingLevel
  object LoggingLevel {
    case object JOB   extends LoggingLevel
    case object DEBUG extends LoggingLevel
    case object INFO  extends LoggingLevel
  }
  case class EtlJob23Props(
      ratings_input_path: String = "",
      ratings_output_dataset: String = "test",
      ratings_output_table_name: String = "ratings",
      job_send_slack_notification: Boolean = true,
      job_enable_db_logging: Boolean = false,
      job_notification_level: LoggingLevel = LoggingLevel.INFO
  )
  case class Student(id: String, name: String, `class`: Option[String])
  case class HttpBinResponse(args: Map[String, String], headers: Map[String, String], origin: String, url: String)
}
