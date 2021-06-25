package etlflow.log

import etlflow.EtlJobProps
import etlflow.common.DateTimeFunctions.{getCurrentTimestamp, getTimeDifferenceAsString}
import etlflow.db.{DBApi, DBEnv}
import etlflow.json.{JsonApi, JsonEnv}
import zio.ZIO

private[etlflow] class JobLogger(job_name: String, job_properties: EtlJobProps, job_run_id: String, is_master:String, slack: Option[SlackLogger]) extends ApplicationLogger {

  def logStart(start_time: Long, job_type: String): ZIO[DBEnv with JsonEnv, Throwable, Unit] = {
    for{
      properties <- JsonApi.convertToJsonJacksonByRemovingKeys(job_properties, List.empty)
      _          = logger.info("Logging job start in db")
      _          <- DBApi.insertJobRun(job_run_id, job_name, properties, job_type, is_master, start_time)
    } yield ()
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
    val elapsed_time = getTimeDifferenceAsString(start_time, getCurrentTimestamp)
    DBApi.updateJobRun(job_run_id, job_status, elapsed_time)
  }
}


