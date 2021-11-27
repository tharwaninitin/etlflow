package etlflow.log

import etlflow.core.StepEnv
import etlflow.schema.LoggingLevel
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{UIO, ZIO}

object JobExecutor {
  def apply(job_name: String, jri: String, job: ZIO[StepEnv, Throwable, Unit], job_notification_level: LoggingLevel)
  : ZIO[SlackEnv with Blocking with Clock, Throwable, Unit] = {
    (for {
      job_start_time <- UIO.succeed(getCurrentTimestamp)
      _ <- LoggerApi.setJobRunId(jri)
      _ <- LoggerApi.jobLogStart(job_start_time, "GenericEtlJob", job_name, "{}", "false")
      _ <- job.foldM(
        ex => LoggerApi.jobLogEnd(job_start_time, jri, job_name, Some(ex)).unit,
        _ => LoggerApi.jobLogEnd(job_start_time, jri, job_name)
      )
    } yield ()).provideSomeLayer[SlackEnv with Blocking with Clock](etlflow.log.Implementation.live ++ etlflow.log.ConsoleImplementation.live ++ etlflow.log.DBNoLogImplementation())
  }
}
