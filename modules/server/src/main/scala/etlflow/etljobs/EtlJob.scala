package etlflow.etljobs

import etlflow._
import etlflow.core.CoreLogEnv
import etlflow.log.LogApi
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.ZIO

trait EtlJob[EJP <: EtlJobProps] extends ApplicationLogger {

  def job: ZIO[CoreLogEnv, Throwable, Unit]
  val job_properties: EJP
  var job_name: String = getClass.getName

  final def execute(args: String = "{}"): ZIO[CoreLogEnv, Throwable, Unit] = {
    for {
      _    <- LogApi.logJobStart(job_name, args, getCurrentTimestamp)
      _    <- job.foldM(
                 ex => LogApi.logJobEnd(job_name, args, getCurrentTimestamp, Some(ex)),
                 _  => LogApi.logJobEnd(job_name, args, getCurrentTimestamp)
             )
    } yield ()
  }
}
