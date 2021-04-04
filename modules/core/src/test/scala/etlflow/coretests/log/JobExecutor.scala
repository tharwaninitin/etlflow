package etlflow.coretests.log

import etlflow.log.EtlLogger.{JobLoggerEnv, logError, logInit, logSuccess}
import etlflow.log.SlackLogger
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobProps, JobLogger, StepEnv, StepLogger}
import zio.{UIO, ZEnv, ZIO, ZLayer}

object JobExecutor {
  def slack(job_name: String, slack_env: String, slack_url: String, job_props: EtlJobProps, job: ZIO[StepEnv, Throwable, Unit]): ZIO[ZEnv, Throwable, SlackLogger] =
    for {
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
      slack           = SlackLogger(job_name, job_props , slack_env , slack_url)
      job_layer       = ZLayer.succeed(JobLoggerEnv.live(JobLogger(None,slack),""))
      step_layer      = ZLayer.succeed(StepLogger(None,slack))
      _               <- job.provideCustomLayer(step_layer).foldM(
                            ex => logError(job_start_time,ex).orElse(ZIO.unit),
                            _  => logSuccess(job_start_time)
                         ).provideCustomLayer(job_layer)
    } yield slack.get

  def db(job_name: String, job_type: String, job_props: EtlJobProps, job: ZIO[StepEnv, Throwable, Unit]): ZIO[ZEnv, Throwable, Unit] =
    for {
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
      job_layer       = ZLayer.succeed(JobLoggerEnv.live(JobLogger(None,None),job_type))
      step_layer      = ZLayer.succeed(StepLogger(None,None))
      _               <- logInit(job_start_time).provideCustomLayer(job_layer)
      _               <- job.provideCustomLayer(step_layer).foldM(
                            ex => logError(job_start_time,ex).orElse(ZIO.unit),
                            _  => logSuccess(job_start_time)
                         ).provideCustomLayer(job_layer)
    } yield ()
}
