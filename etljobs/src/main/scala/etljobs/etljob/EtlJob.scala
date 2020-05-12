package etljobs.etljob

import com.google.cloud.bigquery.BigQuery
import etljobs.EtlJobProps
import etljobs.bigquery.BigQueryManager
import etljobs.log.{DbLogManager, DbManager, SlackLogManager, SlackManager}
import etljobs.spark.SparkManager
import etljobs.utils.GlobalProperties
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import zio.{BootstrapRuntime, Runtime, UIO, ZIO}
import etljobs.utils.{UtilityFunctions => UF}

trait EtlJob extends BootstrapRuntime with SparkManager with BigQueryManager with DbLogManager with SlackLogManager {

  final val etl_job_logger: Logger = Logger.getLogger(getClass.getName)
  final var job_name: String = "NameNotSet"

  val global_properties: Option[GlobalProperties]
  val job_properties: EtlJobProps

  lazy val spark: SparkSession         = createSparkSession(global_properties)
  lazy val bq: BigQuery                = createBigQuerySession(global_properties)

  val db: Option[DbManager]       = None
  val slack: Option[SlackManager] = None

  private[etljob] val runtime: Runtime[Unit] = Runtime.default

  def printJobInfo(level: String = "info"): Unit
  def getJobInfo(level: String = "info"): List[(String,Map[String,String])]
  def execute(): Unit

  private[etljob] def logJobInit(db: Option[DbManager]): ZIO[Any, Throwable, Long] =
    if (db.isDefined) db.get.updateJobInformation("started","insert") else ZIO.succeed(0)

  private[etljob] def logError(e: Throwable, job_start_time: Long)(db: Option[DbManager]): Throwable = {
    if (slack.isDefined) slack.get.updateJobInformation("failed")
    if (db.isDefined) db.get.updateJobInformation("failed")
    etl_job_logger.error(s"Job completed with failure in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}")
    e
  }

  private[etljob] def logSuccess(job_start_time: Long)(db: Option[DbManager]): ZIO[Any, Throwable, Unit] = {
    for {
      _  <- UIO.succeed(if (slack.isDefined) slack.get.updateJobInformation("pass"))
      _  <- if (db.isDefined) db.get.updateJobInformation("pass") else ZIO.unit
      _  <- UIO.succeed(etl_job_logger.info(s"Job completed successfully in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}"))
    } yield ()
  }
}
