package etlflow.etljobs

import etlflow._
import etlflow.log.LoggerApi
import etlflow.schema.LoggingLevel
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import zio.{UIO, ZIO}

trait GenericEtlJob[EJP <: EtlJobProps] extends EtlJob[EJP] {

  def job: ZIO[JobEnv, Throwable, Unit]
  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {}
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = List.empty
  val job_type = "GenericEtlJob"

  final def execute(job_run_id: Option[String] = None, is_master: Option[String] = None, props: String = "{}"): ZIO[JobEnv, Throwable, Unit] = {
    for {
      job_start_time  <- UIO(getCurrentTimestamp)
      jri             = job_run_id.getOrElse(java.util.UUID.randomUUID.toString)
      master_job      = is_master.getOrElse("true")
      _               <- LoggerApi.setJobRunId(jri)
      _               <- LoggerApi.jobLogStart(job_start_time, job_type, job_name, props, master_job)
      _               <- job.foldM(
                            ex => LoggerApi.jobLogError(job_start_time, jri, job_name, ex),
                            _  => LoggerApi.jobLogSuccess(job_start_time, jri, job_name)
                          )
    } yield ()
  }
}
