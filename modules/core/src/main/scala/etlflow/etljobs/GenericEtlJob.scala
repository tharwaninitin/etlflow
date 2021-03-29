package etlflow.etljobs

import cats.effect.Blocker
import etlflow.log.EtlLogger.JobLogger
import etlflow.log.{DbLogManager, SlackLogManager}
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import etlflow.{EtlJobProps, LoggerResource}
import zio.blocking.Blocking
import zio.internal.Platform
import zio.{Has, ZEnv, ZIO, ZLayer}

trait GenericEtlJob[EJP <: EtlJobProps] extends EtlJob[EJP] {

  def job: ZIO[Has[LoggerResource] with ZEnv, Throwable, Unit]
  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {}
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = List.empty
  val job_type = "GenericEtlJob"

  final def execute(job_run_id: Option[String] = None, is_master: Option[String] = None): ZIO[ZEnv, Throwable, Unit] = {
    (for {
      blocker         <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      job_start_time  = UF.getCurrentTimestamp
      jri             = job_run_id.getOrElse(java.util.UUID.randomUUID.toString)
      master_job      = is_master.getOrElse("true")
      slack           = SlackLogManager.createSlackLogger(job_name, job_properties, config.slack.map(_.env).getOrElse(""),config.slack.map(_.url).getOrElse(""))
      db              <- DbLogManager.create(config, Platform.default.executor.asEC, blocker, job_name + "-Pool", job_name, job_properties, jri, master_job)
      job_log         = JobLogger.live(db.job,job_type,slack)
      step_layer      = ZLayer.succeed(LoggerResource(db.step,slack))
      _               <- job_log.logInit(job_start_time).toManaged_
      _               <- job.provideCustomLayer(step_layer).foldM(
                            ex => job_log.logError(job_start_time,ex),
                            _  => job_log.logSuccess(job_start_time)
                          ).toManaged_
    } yield ()).use_(ZIO.unit)
  }
}
