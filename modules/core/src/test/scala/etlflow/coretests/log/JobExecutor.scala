package etlflow.coretests.log

import etlflow.log.{LoggerApi, SlackLogger}
import etlflow.schema.{LoggingLevel, Slack}
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import etlflow.{CoreEnv, _}
import zio.{RIO, UIO, ZIO}

object JobExecutor {

  val job_type = "GenericEtlJob"

  def apply(job_name: String, slack: Option[Slack], job: ZIO[CoreEnv, Throwable, Unit], job_notification_level:LoggingLevel, job_send_slack_notification:Boolean): RIO[CoreEnv, SlackLogger]
  = {
    for {
      job_start_time  <- UIO.succeed(getCurrentTimestamp)
      jri             = java.util.UUID.randomUUID.toString
      slack_logger    = SlackLogger(slack)
      _               <- LoggerApi.setJobRunId(jri)
      _               <- LoggerApi.jobLogStart(job_start_time, job_type, job_name, "{}", "false")
      _               <- job.foldM(
                            ex => LoggerApi.jobLogError(job_start_time, jri, job_name, ex).unit,
                            _  => LoggerApi.jobLogSuccess(job_start_time, jri, job_name)
                          )
    } yield slack_logger
  }
}
