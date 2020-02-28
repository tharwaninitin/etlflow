package etljobs.log

import etljobs.EtlJobName
import etljobs.etlsteps.EtlStep

trait LogManager {
  var job_name: EtlJobName
  var job_run_id: String

  def finalMessageTemplate(run_env: String, job_name: EtlJobName, exec_date: String, message: String, result: String): String = ""
  def updateStepLevelInformation(
                                  execution_start_time: Long,
                                  etl_step: EtlStep[Unit,Unit],
                                  state_status: String,
                                  notification_level:String,
                                  error_message: Option[String] = None
                                ): Unit
  def sendNotification(result: String, start_time: Long): Unit = ()
  def updateJobInformation(job_run_id: String, state: String): Unit = ()
}
