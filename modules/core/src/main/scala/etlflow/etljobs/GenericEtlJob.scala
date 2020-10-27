package etlflow.etljobs

import cats.effect.Blocker
import etlflow.{EtlJobProps, LoggerResource}
import etlflow.log.EtlLogger.JobLogger
import etlflow.log.{DbLogManager, SlackLogManager}
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import zio.blocking.Blocking
import zio.internal.Platform
import zio.{Has, Task, UIO, ZEnv, ZIO, ZLayer, ZManaged}

trait GenericEtlJob[+EJP <: EtlJobProps] extends EtlJob[EJP] {

  def job: ZIO[Has[LoggerResource] with ZEnv, Throwable, Unit]
  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {}
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = List.empty
  val job_type = "GenericEtlJob"

  final def execute(): ZIO[ZEnv, Throwable, Unit] = {
    (for {
      job_status_ref  <- job_status.toManaged_
      resource        <- logger_resource
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

  def getCredentials[T : Manifest](name: String): ZIO[Blocking, Throwable, T] = {
    logger_resource.use{res =>
      if (res.db.isDefined)
        res.db.get.getCredentials[T](name)
      else
        Task.fail(new RuntimeException("db logging not enabled"))
    }
  }

  private[etljobs] lazy val logger_resource: ZManaged[Blocking ,Throwable, LoggerResource] = for {
    blocker    <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
    db         <- DbLogManager.createOptionDbTransactorManagedGP(config, Platform.default.executor.asEC, blocker, job_name + "-Pool", job_name, job_properties)
    slack      <- SlackLogManager.createSlackLogger(job_name, job_properties, config).toManaged_
  } yield LoggerResource(db,slack)
}
