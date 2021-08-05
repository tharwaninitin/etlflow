package etlflow.coretests.log

import etlflow.log.{LoggerApi, SlackLogger}
import etlflow.schema.LoggingLevel
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import etlflow.{CoreEnv, _}
import zio.{UIO, ZIO}

object JobExecutor {

  val job_type = "GenericEtlJob"
  def apply(job_name: String, slack_env: String, slack_url: String, job: ZIO[CoreEnv, Throwable, Unit], job_notification_level:LoggingLevel, job_send_slack_notification:Boolean, host_url:String)
  : ZIO[CoreEnv, Throwable, SlackLogger]
  = {
    for {
      job_start_time  <- UIO.succeed(getCurrentTimestamp)
      jri             = java.util.UUID.randomUUID.toString
      slack           = SlackLogger(job_name, slack_env, slack_url, job_notification_level, job_send_slack_notification,host_url)
      _               <- LoggerApi.setJobRunId(jri)
      _               <- LoggerApi.jobLogStart(job_start_time, job_type,job_name, "{}", "false" )
      _               <- job.foldM(
        ex => LoggerApi.jobLogEnd(job_start_time, Some(ex.getMessage)).unit,
        _  => LoggerApi.jobLogEnd(job_start_time)
      )
    } yield slack.get
  }
}
