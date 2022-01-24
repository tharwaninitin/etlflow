package etlflow.log

import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.{ZEnv, ZIO}

object JobExecutor {
  def apply(job_name: String, job: ZIO[ZEnv with LogEnv, Throwable, Unit]): ZIO[ZEnv with LogEnv, Throwable, Unit] =
    for {
      _ <- LogApi.logJobStart(job_name, "{}", getCurrentTimestamp)
      _ <- job.foldM(
        ex => LogApi.logJobEnd(job_name, "{}", getCurrentTimestamp, Some(ex)),
        _ => LogApi.logJobEnd(job_name, "{}", getCurrentTimestamp)
      )
    } yield ()
}
