package etlflow.etljobs

import etlflow._
import etlflow.core.CoreLogEnv
import etlflow.log.LogWrapperApi
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.{UIO, ZIO}

trait GenericEtlJob[EJP <: EtlJobProps] extends EtlJob[EJP] {

  def job: ZIO[CoreLogEnv, Throwable, Unit]
  val job_type = "GenericEtlJob"

  final def execute(job_run_id: Option[String] = None, is_master: Option[String] = None, props: String = "{}"): ZIO[CoreLogEnv, Throwable, Unit] = {
    for {
      job_start_time  <- UIO(getCurrentTimestamp)
      jri             = job_run_id.getOrElse(java.util.UUID.randomUUID.toString)
      master_job      = is_master.getOrElse("true")
      _               <- LogWrapperApi.setJobRunId(jri)
      _               <- LogWrapperApi.jobLogStart(job_start_time, job_type, job_name, props, master_job)
      _               <- job.foldM(
                            ex => LogWrapperApi.jobLogEnd(job_start_time, jri, job_name, Some(ex)),
                            _  => LogWrapperApi.jobLogEnd(job_start_time, jri, job_name)
                          )
    } yield ()
  }
}
