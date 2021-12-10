package etlflow.etljobs

import etlflow._
import etlflow.core.CoreLogEnv
import etlflow.log.LogApi
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.{UIO, ZIO}

trait EtlJob[EJP <: EtlJobProps] extends ApplicationLogger {

  def job: ZIO[CoreLogEnv, Throwable, Unit]
  val job_properties: EJP
  var job_name: String = getClass.getName

  final def execute(job_run_id: Option[String] = None, args: String = "{}"): ZIO[CoreLogEnv, Throwable, Unit] = {
    for {
      jri  <- UIO(job_run_id.getOrElse(java.util.UUID.randomUUID.toString))
      _    <- LogApi.setJobRunId(jri)
      _    <- LogApi.logJobStart(jri, job_name, args, getCurrentTimestamp)
      _    <- job.foldM(
                 ex => LogApi.logJobEnd(jri, job_name, args, getCurrentTimestamp, Some(ex)),
                 _  => LogApi.logJobEnd(jri, job_name, args, getCurrentTimestamp)
             )
    } yield ()
  }
}
