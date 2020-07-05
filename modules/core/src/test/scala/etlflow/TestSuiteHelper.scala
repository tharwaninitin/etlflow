package etlflow

import java.io.FileInputStream

import cats.effect.Blocker
import org.slf4j.{Logger, LoggerFactory}
import ch.qos.logback.classic.{Level, Logger => LBLogger}
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import doobie.Transactor
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor.Aux
import etlflow.spark.SparkManager
import etlflow.utils.GlobalProperties
import software.amazon.awssdk.regions.Region
import zio.{Runtime, Task, ZEnv}
import zio.interop.catz._
import scala.util.Try

trait TestSuiteHelper extends SparkManager {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  lazy val spark_logger: LBLogger = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  spark_logger.setLevel(Level.WARN)
  lazy val spark_jetty_logger: LBLogger = LoggerFactory.getLogger("org.spark_project.jetty").asInstanceOf[LBLogger]
  spark_jetty_logger.setLevel(Level.WARN)

  lazy val gcs_bucket: String              = sys.env.getOrElse("GCS_BUCKET","...")
  lazy val s3_bucket: String               = sys.env.getOrElse("S3_BUCKET","...")
  lazy val s3_region: Region               = Region.AP_SOUTH_1
  lazy val bq: BigQuery                    = {
    val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(
      new FileInputStream(sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS","<not_set>"))
    )
    BigQueryOptions.newBuilder().setCredentials(credentials).build().getService
  }
  val canonical_path: String          = new java.io.File(".").getCanonicalPath
  override val global_properties: Option[GlobalProperties] =
    Try(new GlobalProperties(canonical_path + "/modules/core/src/test/resources/loaddata.properties") {}).toOption
  val file                            = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"

  def transactor(url: String, user: String, pwd: String): Aux[Task, Unit]
  = Transactor.fromDriverManager[Task](
    global_properties.get.log_db_driver,     // driver classname
    url,        // connect URL (driver-specific)
    user,       // user
    pwd,        // password
    Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
  )

  val runtime: Runtime[ZEnv]          = Runtime.default
}
