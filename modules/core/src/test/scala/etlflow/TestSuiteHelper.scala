package etlflow

import cats.effect.Blocker
import ch.qos.logback.classic.{Level, Logger => LBLogger}
import doobie.Transactor
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor.Aux
import etlflow.utils.{Config, GlobalProperties}
import org.slf4j.{Logger, LoggerFactory}
import pureconfig.ConfigSource
import software.amazon.awssdk.regions.Region
import zio.interop.catz._
import zio.{Runtime, Task, ZEnv}

import scala.util.Try
import pureconfig.generic.auto._
trait TestSuiteHelper {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  lazy val spark_logger: LBLogger = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  spark_logger.setLevel(Level.WARN)
  lazy val spark_jetty_logger: LBLogger = LoggerFactory.getLogger("org.spark_project.jetty").asInstanceOf[LBLogger]
  spark_jetty_logger.setLevel(Level.WARN)

  lazy val gcs_bucket: String              = sys.env.getOrElse("GCS_BUCKET","...")
  lazy val s3_bucket: String               = sys.env.getOrElse("S3_BUCKET","...")
  lazy val s3_region: Region               = Region.AP_SOUTH_1
  val canonical_path: String               = new java.io.File(".").getCanonicalPath
//  override val global_properties: Option[GlobalProperties] =
//    Try(new GlobalProperties(canonical_path + "/modules/core/src/test/resources/loaddata.properties") {}).toOption

  val file                            = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"

  val global_properties: Config = ConfigSource.default.loadOrThrow[Config]

  def transactor(url: String, user: String, pwd: String): Aux[Task, Unit]
  = Transactor.fromDriverManager[Task](
    global_properties.dbLog.driver,     // driver classname
    url,        // connect URL (driver-specific)
    user,       // user
    pwd,        // password
    Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
  )

  val runtime: Runtime[ZEnv]          = Runtime.default
}
