package etlflow.etljobs

import etlflow.LoggerResource
import etlflow.jdbc.DbManager
import etlflow.log.{DbLogManager, SlackLogManager}
import etlflow.utils.{UtilityFunctions => UF}
import zio.internal.Platform
import zio.{Task, UIO, ZIO, ZManaged}

trait GenericEtlJobWithLogging extends EtlJob with DbManager {

  def job(implicit resource: LoggerResource): Task[Unit]
  def printJobInfo(level: String = "info"): Unit = {}
  def getJobInfo(level: String = "info"): List[(String,Map[String,String])] = List.empty

  final def execute(): ZIO[Any, Throwable, Unit] = {
    (for {
      job_status_ref  <- job_status.toManaged_
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp).toManaged_
      resource        <- logger_resource
      _               <- (job_status_ref.set("started") *> logJobInit(resource)).toManaged_
      _               <- job(resource).foldM(
                            ex => job_status_ref.set("failed") *> logJobError(ex,job_start_time)(resource),
                            _  => job_status_ref.set("success") *> logJobSuccess(job_start_time)(resource)
                          ).toManaged_
    } yield ()).use_(ZIO.unit)
  }

  private[etljobs] lazy val logger_resource: ZManaged[Any, Throwable, LoggerResource] = for {
    transactor      <- createDbTransactorManagedGP(global_properties,Platform.default.executor.asEC,job_name+"-Pool")
    db              <- DbLogManager.createDbLoggerManaged(transactor,job_name,job_properties)
    slack           <- SlackLogManager.createSlackLogger(job_name,job_properties,global_properties).toManaged_
  } yield LoggerResource(Option(db),slack)

  private[etljobs] def logJobInit(res: LoggerResource): Task[Long] =
    if (res.db.isDefined) res.db.get.updateJobInformation("started","insert") else ZIO.succeed(0)

  private[etljobs] def logJobError(e: Throwable, job_start_time: Long)(res: LoggerResource): Task[Unit] = {
    if (res.slack.isDefined) res.slack.get.updateJobInformation("failed")
    etl_job_logger.error(s"Job completed with failure in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}")
    if (res.db.isDefined) res.db.get.updateJobInformation("failed").as(()) *> Task.fail(e) else Task.fail(e)
  }

  private[etljobs] def logJobSuccess(job_start_time: Long)(res: LoggerResource): Task[Unit] = {
    for {
      _  <- UIO.succeed(if (res.slack.isDefined) res.slack.get.updateJobInformation("pass"))
      _  <- if (res.db.isDefined) res.db.get.updateJobInformation("pass") else ZIO.unit
      _  <- UIO.succeed(etl_job_logger.info(s"Job completed successfully in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}"))
    } yield ()
  }
}
