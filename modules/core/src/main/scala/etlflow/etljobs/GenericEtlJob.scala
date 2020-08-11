package etlflow.etljobs

import cats.effect.Blocker
import etlflow.LoggerResource
import etlflow.log.EtlLogger.JobLogger
import etlflow.log.{DbLogManager, SlackLogManager}
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import zio.blocking.Blocking
import zio.internal.Platform
import zio.{UIO, ZIO, ZManaged}

trait GenericEtlJob extends EtlJob {

  val job: ZIO[LoggerResource, Throwable, Unit]
  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {}
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = List.empty

  final def execute(): ZIO[Any, Throwable, Unit] = {
    (for {
      job_status_ref  <- job_status.toManaged_
      resource        <- logger_resource
      log             = JobLogger.live(resource)
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp).toManaged_
      _               <- (job_status_ref.set("started") *> log.logInit(job_start_time)).toManaged_
      _               <- job.provide(resource).foldM(
                            ex => job_status_ref.set("failed") *> log.logError(job_start_time,ex),
                            _  => job_status_ref.set("success") *> log.logSuccess(job_start_time)
                          ).toManaged_
    } yield ()).use_(ZIO.unit).provideLayer(Blocking.live)
  }

  private[etljobs] lazy val logger_resource: ZManaged[Blocking ,Throwable, LoggerResource] = for {
    blocker    <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
    db         <- DbLogManager.createOptionDbTransactorManagedGP(globalProperties, Platform.default.executor.asEC, blocker, job_name + "-Pool", job_name, job_properties)
    slack      <- SlackLogManager.createSlackLogger(job_name, job_properties, globalProperties).toManaged_
  } yield LoggerResource(db,slack)
}
