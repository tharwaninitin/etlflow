package etlflow.log

import etlflow.JobEnv
import etlflow.crypto.CryptoEnv
import etlflow.db.DBEnv
import etlflow.json.JsonEnv
import etlflow.schema.LoggingLevel
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{UIO, ZIO}

object JobExecutor {

  val job_type = "GenericEtlJob"

  def apply(job_name: String, jri: String, job: ZIO[JobEnv, Throwable, Unit], job_notification_level: LoggingLevel)
  : ZIO[DBEnv with SlackEnv with JsonEnv with CryptoEnv with Blocking with Clock, Throwable, Unit] = {
    (for {
      job_start_time <- UIO.succeed(getCurrentTimestamp)
      _ <- LoggerApi.setJobRunId(jri)
      _ <- LoggerApi.jobLogStart(job_start_time, job_type, job_name, "{}", "false")
      _ <- job.foldM(
        ex => LoggerApi.jobLogError(job_start_time, jri, job_name, ex).unit,
        _ => LoggerApi.jobLogSuccess(job_start_time, jri, job_name)
      )
    } yield ()).provideSomeLayer[DBEnv with SlackEnv with JsonEnv with CryptoEnv with Blocking with Clock](etlflow.log.Implementation.live)
  }
}
