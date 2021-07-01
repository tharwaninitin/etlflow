package etlflow.log

import etlflow.db.{DBApi, DBEnv}
import etlflow.json.JsonEnv
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getTimeDifferenceAsString}
import zio.{Task, ZIO}

private[etlflow] class JobLogger(job_name: String, props: String, job_run_id: String, is_master:String, slack: Option[SlackLogger]) extends ApplicationLogger {

  def logStart(start_time: Long, job_type: String): ZIO[DBEnv with JsonEnv, Throwable, Unit] = {
    for{
      _          <- Task(logger.info("Logging job start in db"))
      _          <- DBApi.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
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


