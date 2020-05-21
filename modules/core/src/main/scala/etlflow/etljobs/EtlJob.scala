package etlflow.etljobs

import etlflow.jdbc.DbManager
import etlflow.log.{DbLogManager, SlackLogManager}
import etlflow.utils.{GlobalProperties, UtilityFunctions => UF}
import etlflow.{EtlJobProps, LoggerResource}
import org.apache.log4j.Logger
import zio.{BootstrapRuntime, Task, UIO, ZIO, ZManaged}

trait EtlJob extends BootstrapRuntime with DbManager {

  final val etl_job_logger: Logger = Logger.getLogger(getClass.getName)

  var job_name: String = "NameNotSet"
  val global_properties: Option[GlobalProperties]
  val job_properties: EtlJobProps

  def etlJob(implicit resource: LoggerResource): Task[Unit]
  def printJobInfo(level: String = "info"): Unit = {}
  def getJobInfo(level: String = "info"): List[(String,Map[String,String])] = List.empty

  final def execute(): ZIO[Any, Throwable, Unit] = {
    (for {
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp).toManaged_
      resource        <- logger_resource
      _               <- logJobInit(resource).toManaged_
      _               <- etlJob(resource).mapError(e => logJobError(e,job_start_time)(resource)).toManaged_
      _               <- logJobSuccess(job_start_time)(resource).toManaged_
    } yield ()).use_(ZIO.unit)
  }

  private[etljobs] lazy val logger_resource: ZManaged[Any, Throwable, LoggerResource] = for {
    transactor      <- createDbTransactorManagedGP(global_properties,platform.executor.asEC,job_name+"-Pool")
    db              <- DbLogManager.createDbLoggerManaged(transactor,job_name,job_properties)
    slack           <- SlackLogManager.createSlackLogger(job_name,job_properties,global_properties).toManaged_
  } yield LoggerResource(Option(db),slack)

  private[etljobs] def logJobInit(res: LoggerResource): ZIO[Any, Throwable, Long] =
    if (res.db.isDefined) res.db.get.updateJobInformation("started","insert") else ZIO.succeed(0)

  private[etljobs] def logJobError(e: Throwable, job_start_time: Long)(res: LoggerResource): Throwable = {
    if (res.slack.isDefined) res.slack.get.updateJobInformation("failed")
    if (res.db.isDefined) res.db.get.updateJobInformation("failed")
    etl_job_logger.error(s"Job completed with failure in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}")
    e
  }

  private[etljobs] def logJobSuccess(job_start_time: Long)(res: LoggerResource): ZIO[Any, Throwable, Unit] = {
    for {
      _  <- UIO.succeed(if (res.slack.isDefined) res.slack.get.updateJobInformation("pass"))
      _  <- if (res.db.isDefined) res.db.get.updateJobInformation("pass") else ZIO.unit
      _  <- UIO.succeed(etl_job_logger.info(s"Job completed successfully in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}"))
    } yield ()
  }
}
