package examples.schema

import etlflow.EtlJobProps
import etlflow.utils.LoggingLevel

sealed trait MyEtlJobProps extends EtlJobProps

object MyEtlJobProps {

  case class EtlJob1TriggerProps() extends MyEtlJobProps
  case class EtlJob1Props (
                            ratings_input_path: List[String] = List(""),
                            ratings_intermediate_path: String = "",
                            ratings_output_dataset: String = "",
                            ratings_output_table_name: String = "",
                            ratings_output_file_name: Option[String] = Some("ratings.orc"),
                            override val job_schedule: String = "0 */15 * * * ?",
                            override val job_deploy_mode: String = "local",
                            override val job_enable_db_logging: Boolean = false
                          ) extends MyEtlJobProps
  case class EtlJob23Props (
                            ratings_input_path: String = "",
                            ratings_output_dataset: String = "",
                            ratings_output_table_name: String = "",
                            override val job_send_slack_notification: Boolean = true,
                            override val job_notification_level: LoggingLevel = LoggingLevel.INFO,
                          ) extends MyEtlJobProps
  case class EtlJob4Props() extends MyEtlJobProps
  case class EtlJob5Props (
                            ratings_input_path: List[String] = List(""),
                            ratings_output_table: String = "",
                          ) extends MyEtlJobProps
}
