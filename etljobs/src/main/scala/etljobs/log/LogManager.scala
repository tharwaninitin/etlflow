package etljobs.log

import etljobs.{EtlJobName, EtlProps}
import etljobs.etlsteps.EtlStep
import org.apache.log4j.Logger

trait LogManager {
  var job_name: EtlJobName
  var job_run_id: String
  var log_level: String = "info"
  val lm_logger: Logger = Logger.getLogger(getClass.getName)

  def finalMessageTemplate(run_env: String, job_name: EtlJobName, exec_date: String, message: String, result: String): String = ""
  def updateStepLevelInformation(
                                  execution_start_time: Long,
                                  etl_step: EtlStep[Unit,Unit],
                                  state_status: String,
                                  notification_level:String,
                                  error_message: Option[String] = None,
                                  mode: String = "update"
                                ): Unit
  def sendNotification(result: String, start_time: Long): Unit = ()
  def updateJobInformation(status: String, etlProps: Option[EtlProps] = None, mode: String = "update"): Unit = ()
}
