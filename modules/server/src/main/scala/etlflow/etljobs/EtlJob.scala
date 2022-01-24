package etlflow.etljobs

import etlflow._
import etlflow.log.{LogApi, LogEnv}
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.{ZEnv, ZIO}

trait EtlJob[EJP <: EtlJobProps] extends ApplicationLogger {

  def job: ZIO[ZEnv with LogEnv, Throwable, Unit]
  val job_properties: EJP
  var job_name: String = getClass.getName

  final def execute(args: String = "{}"): ZIO[ZEnv with LogEnv, Throwable, Unit] =
    for {
      _ <- LogApi.logJobStart(job_name, args, getCurrentTimestamp)
      _ <- job.foldM(
        ex => LogApi.logJobEnd(job_name, args, getCurrentTimestamp, Some(ex)),
        _ => LogApi.logJobEnd(job_name, args, getCurrentTimestamp)
      )
    } yield ()
}
