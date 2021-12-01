package etlflow.etljobs

import etlflow.core.StepEnv
import etlflow.schema.LoggingLevel
import etlflow.utils.ApplicationLogger
import etlflow.EtlJobProps
import zio._

trait EtlJob[EJP <: EtlJobProps] extends ApplicationLogger {

  val job_properties: EJP
  var job_name: String = getClass.getName
  var job_enable_db_logging: Boolean = true
  var job_send_slack_notification: Boolean = false
  var job_notification_level: LoggingLevel = LoggingLevel.INFO

  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])]
  def execute(job_run_id: Option[String] = None, is_master: Option[String] = None, props: String = "{}"): ZIO[StepEnv, Throwable, Unit]
}
