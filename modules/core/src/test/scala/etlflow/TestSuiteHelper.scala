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
import org.apache.spark.sql.SparkSession
import zio.{Runtime, Task, ZEnv}
import zio.interop.catz._

trait TestSuiteHelper extends SparkManager {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  lazy val spark_logger: LBLogger = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  spark_logger.setLevel(Level.WARN)
  lazy val spark_jetty_logger: LBLogger = LoggerFactory.getLogger("org.spark_project.jetty").asInstanceOf[LBLogger]
  spark_jetty_logger.setLevel(Level.WARN)

  val gcs_bucket: String              = sys.env("GCS_BUCKET")
  val s3_bucket: String               = sys.env("S3_BUCKET")
  val bq: BigQuery                    = {
    val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(
      new FileInputStream(sys.env("GOOGLE_APPLICATION_CREDENTIALS"))
    )
    BigQueryOptions.newBuilder().setCredentials(credentials).build().getService
  }
  val canonical_path: String          = new java.io.File(".").getCanonicalPath
  val global_props: GlobalProperties  = new GlobalProperties(canonical_path + "/modules/core/src/test/resources/loaddata.properties") {}
  lazy val spark: SparkSession        = createSparkSession(Some(global_props))
  val file                            = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"

  def transactor(url: String, user: String, pwd: String): Aux[Task, Unit]
  = Transactor.fromDriverManager[Task](
    global_props.log_db_driver,     // driver classname
    url,        // connect URL (driver-specific)
    user,       // user
    pwd,        // password
    Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
  )

  val runtime: Runtime[ZEnv]          = Runtime.default
}
