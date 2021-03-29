package etlflow.coretests.log

import etlflow.log.EtlLogger.{JobLogger, logError, logInit, logSuccess}
import etlflow.log.SlackLogManager
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobProps, LoggerResource}
import zio.{Has, UIO, ZEnv, ZIO, ZLayer}

object JobExecutor {
  def slack(
           job_name: String,
           slack_env: String,
           slack_url: String,
           job_props: EtlJobProps,
           job: ZIO[Has[LoggerResource] with ZEnv, Throwable, Unit]
         ): ZIO[ZEnv, Throwable, SlackLogManager] =
    for {
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
      slack           = SlackLogManager.createSlackLogger(job_name, job_props , slack_env , slack_url)
      job_layer       = ZLayer.succeed(JobLogger.live(None,"",slack))
      step_layer      = ZLayer.succeed(LoggerResource(None,slack))
      _               <- job.provideCustomLayer(step_layer).foldM(
                        ex => logError(job_start_time,ex).orElse(ZIO.unit),
                        _  => logSuccess(job_start_time)
                      ).provideCustomLayer(job_layer)
    } yield slack.get
}
