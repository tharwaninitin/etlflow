package etlflow.log

import etlflow.EtlJobProps
import etlflow.jdbc.{DB, DBEnv}
import etlflow.utils.{JsonJackson, UtilityFunctions => UF}
import zio.ZIO

class JobLogger(job_name: String, job_properties: EtlJobProps, job_run_id: String, is_master:String, slack: Option[SlackLogger]) extends ApplicationLogger {

  def logStart(start_time: Long, job_type: String): ZIO[DBEnv, Throwable, Unit] = {
    val properties = JsonJackson.convertToJsonByRemovingKeys(job_properties, List.empty)
    logger.info("Logging job start in db")
    DB.insertJobRun(job_run_id, job_name, properties, job_type, is_master, start_time)
  }

  def logEnd(start_time: Long, error_message: Option[String] = None):  ZIO[DBEnv, Throwable, Unit]  = {
    var job_status = ""
    if (error_message.isDefined) {
      slack.foreach(_.logJobEnd(start_time, error_message))
      job_status = "failed with error: " + error_message.get
    } else {
      slack.foreach(_.logJobEnd(start_time))
      job_status = "pass"
    }
    logger.info(s"Logging job completion in db with status $job_status")
    val elapsed_time = UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)
    DB.updateJobRun(job_run_id, job_status, elapsed_time)
  }
}


