package etlflow.log

import etlflow.{EtlJobProps, LoggerResource}
import etlflow.log.EtlLogger.JobLogger
import zio.{Has, Task, UIO, ZIO, ZLayer, ZEnv}
import etlflow.utils.{UtilityFunctions => UF}

object JobExecutor {
  def slack(
           job_name: String,
           slack_env: String,
           slack_url: String,
           job_props: EtlJobProps,
           job: ZIO[Has[LoggerResource] with ZEnv, Throwable, Unit]
         ): ZIO[ZEnv, Throwable, SlackLogManager] =
    for {
      slack           <- Task(SlackLogManager.createSlackLogger(job_name, job_props , slack_env , slack_url))
      resource        = LoggerResource(None,Some(slack))
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
      log             = JobLogger.live(resource)
      resourceLayer   = ZLayer.succeed(resource)
      _               <- job.provideCustomLayer(resourceLayer).foldM(
                        ex => log.logError(job_start_time,ex).orElse(ZIO.unit),
                        _  => log.logSuccess(job_start_time)
                      )
    } yield slack
}
