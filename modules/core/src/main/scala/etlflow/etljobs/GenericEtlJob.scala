package etlflow.etljobs

import etlflow.jdbc.DBServerEnv
import etlflow.log.{DbJobLogger}
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import etlflow.{EtlJobProps, JobEnv, StepEnv}
import zio.{Task, UIO, ZIO, ZLayer}
import etlflow.log.DbStepLogger.StepReq
trait GenericEtlJob[EJP <: EtlJobProps] extends EtlJob[EJP] {

  def job: ZIO[StepEnv, Throwable, Unit]
  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {}
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = List.empty
  val job_type = "GenericEtlJob"

  final def execute(job_run_id: Option[String] = None, is_master: Option[String] = None): ZIO[JobEnv, Throwable, Unit] = {
    for {
      job_start_time  <- UIO(UF.getCurrentTimestamp)
      jri             = job_run_id.getOrElse(java.util.UUID.randomUUID.toString)
      master_job      = is_master.getOrElse("true")
      slack_env       = config.slack.map(_.env).getOrElse("")
      slack_url       = config.slack.map(_.url).getOrElse("")
      host_url        = config.host.getOrElse("http://localhost:8080/#")  + "/JobRunDetails/" + jri
      dbJob           = new DbJobLogger(job_name, job_properties, jri, master_job)
      step_layer      = ZLayer.succeed(StepReq(jri))
      _               <- dbJob.logStart(job_start_time, job_type)
      _               <- job.provideSomeLayer[JobEnv](step_layer).foldM(
                            ex => dbJob.logEnd(job_start_time, Some(ex.getMessage)).unit *> Task.fail(ex),
                            _  => dbJob.logEnd(job_start_time)
                          )
    } yield ()
  }
}
