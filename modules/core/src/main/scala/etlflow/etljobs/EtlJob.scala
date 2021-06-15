package etlflow.etljobs

import etlflow.log.ApplicationLogger
import etlflow.utils.{Configuration, LoggingLevel}
import etlflow.{EtlJobProps, JobEnv}
import zio._

trait EtlJob[EJP <: EtlJobProps] extends Configuration with ApplicationLogger {

  val job_properties: EJP

  var job_name: String = getClass.getName
  var job_enable_db_logging: Boolean        = true
  var job_send_slack_notification: Boolean  = false
  var job_notification_level: LoggingLevel  = LoggingLevel.INFO

  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])]
  def execute(job_run_id: Option[String] = None, is_master: Option[String] = None): ZIO[JobEnv, Throwable, Unit]
}
