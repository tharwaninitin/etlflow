package examples.schema

import etlflow.EtlJobProps
import etlflow.utils.{Executor, LoggingLevel}
import etlflow.utils.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}

import scala.concurrent.duration.Duration
import scala.concurrent.duration._
sealed trait MyEtlJobProps extends EtlJobProps

object MyEtlJobProps {

  val local_subprocess = LOCAL_SUBPROCESS("examples/target/docker/stage/opt/docker/bin/load-data",heap_min_memory = "-Xms100m",heap_max_memory = "-Xms100m")

  val dataproc   = DATAPROC("project-name","region","endpoint","cluster-name")

  val kubernetes = KUBERNETES(
    "etlflow:0.7.19",
    "default",
    Map(
      "GOOGLE_APPLICATION_CREDENTIALS"-> Option("<cred_file>"),
      "LOG_DB_URL"-> Option("jdbc:postgresql://host.docker.internal:5432/postgres"),
      "LOG_DB_USER"-> Option("<username>"),
      "LOG_DB_PWD"-> Option("<pwd>"),
      "LOG_DB_DRIVER"-> Option("org.postgresql.Driver")
    )
  )
  case class SampleProps(
                          override val job_deploy_mode: Executor = dataproc,
                          override val job_schedule: String = "0 */15 * * * ?"
                        ) extends MyEtlJobProps
  case class LocalSampleProps(
                               override val job_schedule: String = "0 */15 * * * ?",
                               override val job_max_active_runs: Int = 1,
                               override val job_retries: Int = 3,
                               override val job_retry_delay_in_minutes: Int = 1
                             ) extends MyEtlJobProps
  case class EtlJob1Props (
                            ratings_input_path: List[String] = List(""),
                            ratings_intermediate_path: String = "",
                            ratings_output_dataset: String = "",
                            ratings_output_table_name: String = "",
                            ratings_output_file_name: Option[String] = Some("ratings.orc"),
                            override val job_deploy_mode: Executor = kubernetes,
                            override val job_enable_db_logging: Boolean = false
                          ) extends MyEtlJobProps
  case class EtlJob23Props (
                             ratings_input_path: String = "",
                             ratings_output_dataset: String = "",
                             ratings_output_table_name: String = "",
                             override val job_send_slack_notification: Boolean = true,
                             override val job_schedule: String = "0 0 10 ? * 4",
                             override val job_notification_level: LoggingLevel = LoggingLevel.INFO,
                           ) extends MyEtlJobProps
  case class EtlJob4Props() extends MyEtlJobProps


  case class EtlJob5Props (
                            ratings_input_path: List[String] = List(""),
                            override val job_schedule: String = "0 0 5,6 ? * *",
                            ratings_output_table: String = "",
                          ) extends MyEtlJobProps

  case class EtlJob6Props(override val job_deploy_mode: Executor = local_subprocess) extends MyEtlJobProps
}
