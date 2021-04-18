package etlflow.coretests.log

import etlflow.log.EtlLogger.{JobLoggerImpl, logError, logInit, logSuccess}
import etlflow.log.{DbLogger, SlackLogger}
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobProps, JobLogger, StepEnv, StepLogger}
import zio.{UIO, ZEnv, ZIO, ZLayer}
import etlflow.utils.LoggingLevel

object JobExecutor {
  def slack(job_name: String, slack_env: String, slack_url: String, job: ZIO[StepEnv, Throwable, Unit],job_notification_level:LoggingLevel,job_send_slack_notification:Boolean): ZIO[ZEnv, Throwable, SlackLogger] =
    for {
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
      slack           = SlackLogger(job_name, slack_env, slack_url, job_notification_level, job_send_slack_notification)
      job_layer       = ZLayer.succeed(JobLoggerImpl(JobLogger(None,slack),""))
      step_layer      = ZLayer.succeed(StepLogger(None,slack))
      _               <- job.provideCustomLayer(step_layer).foldM(
                            ex => logError(job_start_time,ex).orElse(ZIO.unit),
                            _  => logSuccess(job_start_time)
                         ).provideCustomLayer(job_layer)
    } yield slack.get

  def db(job_name: String, job_type: String, job_props: EtlJobProps, job: ZIO[StepEnv, Throwable, Unit]): ZIO[ZEnv, Throwable, Unit] =
    for {
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
      job_layer       = ZLayer.succeed(JobLoggerImpl(JobLogger(None,None),job_type))
      step_layer      = ZLayer.succeed(StepLogger(None,None))
      _               <- logInit(job_start_time).provideCustomLayer(job_layer)
      _               <- job.provideCustomLayer(step_layer).foldM(
                            ex => logError(job_start_time,ex).orElse(ZIO.unit),
                            _  => logSuccess(job_start_time)
                         ).provideCustomLayer(job_layer)
    } yield ()
}
