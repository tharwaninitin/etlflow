package etlflow.log

import etlflow.core.CoreLogEnv
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.ZIO

object JobExecutor {
  def apply(job_name: String, jri: String, job: ZIO[CoreLogEnv, Throwable, Unit]): ZIO[CoreLogEnv, Throwable, Unit] = {
    (for {
      _ <- LogApi.setJobRunId(jri)
      _ <- LogApi.logJobStart(jri, job_name, "{}", getCurrentTimestamp)
      _ <- job.foldM(
        ex => LogApi.logJobEnd(jri, job_name, "{}", getCurrentTimestamp, Some(ex)),
        _ => LogApi.logJobEnd(jri, job_name, "{}", getCurrentTimestamp)
      )
    } yield ())
  }
}
