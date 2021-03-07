package etlflow.etljobs

import cats.effect.Blocker
import etlflow.jdbc.DbManager
import etlflow.{EtlJobProps, LoggerResource}
import etlflow.log.EtlLogger.JobLogger
import etlflow.log.{DbLogManager, SlackLogManager}
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import zio.blocking.Blocking
import zio.internal.Platform
import zio.{Has, UIO, ZEnv, ZIO, ZLayer, ZManaged}

trait GenericEtlJob[EJP <: EtlJobProps] extends EtlJob[EJP] with DbManager {

  def job: ZIO[Has[LoggerResource] with ZEnv, Throwable, Unit]
  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {}
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = List.empty
  val job_type = "GenericEtlJob"

  final def execute(job_run_id:Option[String] = None, is_master:Option[String] = None): ZIO[ZEnv, Throwable, Unit] = {
    (for {
      job_status_ref  <- job_status.toManaged_
      jri             = job_run_id.getOrElse(java.util.UUID.randomUUID.toString)
      master_job      = is_master.getOrElse("true")
      resource        <- logger_resource(jri,master_job)
      log             = JobLogger.live(resource,job_type)
      resourceLayer   = ZLayer.succeed(resource)
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp).toManaged_
      _               <- (job_status_ref.set("started") *> log.logInit(job_start_time)).toManaged_
      _               <- job.provideCustomLayer(resourceLayer).foldM(
                            ex => job_status_ref.set("failed") *> log.logError(job_start_time,ex),
                            _  => job_status_ref.set("success") *> log.logSuccess(job_start_time)
                          ).toManaged_
    } yield ()).use_(ZIO.unit)
  }

  final def getCredentials[T : Manifest](name: String): ZIO[Blocking, Throwable, T] = {
    getDbCredentials[T](name,config.dbLog,scala.concurrent.ExecutionContext.Implicits.global)
  }

  private[etljobs] final def logger_resource(job_run_id:String,is_master:String): ZManaged[Blocking ,Throwable, LoggerResource] = for {
    blocker    <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
    db         <- DbLogManager.createOptionDbTransactorManagedGP(config, Platform.default.executor.asEC, blocker, job_name + "-Pool", job_name, job_properties,job_run_id,is_master)
    slack      <- SlackLogManager.createSlackLogger(job_name, job_properties, config).toManaged_
  } yield LoggerResource(db,slack)
}
