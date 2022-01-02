package etlflow.log

import etlflow.core.CoreLogEnv
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.ZIO

object JobExecutor {
  def apply(job_name: String, job: ZIO[CoreLogEnv, Throwable, Unit]): ZIO[CoreLogEnv, Throwable, Unit] = {
    for {
      _ <- LogApi.logJobStart(job_name, "{}", getCurrentTimestamp)
      _ <- job.foldM(
        ex => LogApi.logJobEnd(job_name, "{}", getCurrentTimestamp, Some(ex)),
        _ => LogApi.logJobEnd(job_name, "{}", getCurrentTimestamp)
      )
    } yield ()
  }
}
