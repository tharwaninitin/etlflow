package etlflow.log

import etlflow.core.CoreLogEnv
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{UIO, ZIO}

object JobExecutor {
  def apply(job_name: String, jri: String, job: ZIO[CoreLogEnv, Throwable, Unit]): ZIO[SlackLogEnv with Blocking with Clock, Throwable, Unit] = {
    (for {
      job_start_time <- UIO.succeed(getCurrentTimestamp)
      _ <- LogWrapperApi.setJobRunId(jri)
      _ <- LogWrapperApi.jobLogStart(job_start_time, "GenericEtlJob", job_name, "{}", "false")
      _ <- job.foldM(
        ex => LogWrapperApi.jobLogEnd(job_start_time, jri, job_name, Some(ex)).unit,
        _ => LogWrapperApi.jobLogEnd(job_start_time, jri, job_name)
      )
    } yield ()).provideSomeLayer[SlackLogEnv with Blocking with Clock](etlflow.log.Implementation.live ++ etlflow.log.ConsoleImplementation.live ++ etlflow.log.DBNoLogImplementation())
  }
}
