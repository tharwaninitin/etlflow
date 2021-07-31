package etlflow.coretests.log

import etlflow.log.{JobLogger, SlackLogger, StepReq}
import etlflow.schema.LoggingLevel
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import etlflow.{JobEnv, _}
import zio.{UIO, ZIO, ZLayer}

object JobExecutor {

  val job_type = "GenericEtlJob"
  def apply(job_name: String, slack_env: String, slack_url: String, job: ZIO[StepEnv, Throwable, Unit], job_notification_level:LoggingLevel, job_send_slack_notification:Boolean, host_url:String)
  : ZIO[JobEnv, Throwable, SlackLogger]
  = {
    for {
      job_start_time  <- UIO.succeed(getCurrentTimestamp)
      jri             = java.util.UUID.randomUUID.toString
      slack           = SlackLogger(job_name, slack_env, slack_url, job_notification_level, job_send_slack_notification,host_url)
      dbJob           = new JobLogger(job_name, "{}", jri, "true", slack)
      step_layer      = ZLayer.succeed(StepReq(jri,slack))
      _               <- dbJob.logStart(job_start_time, job_type)
      _               <- job.provideSomeLayer[JobEnv](step_layer).foldM(
        ex => dbJob.logEnd(job_start_time, Some(ex.getMessage)).unit,
        _  => dbJob.logEnd(job_start_time)
      )
    } yield slack.get
  }
}
