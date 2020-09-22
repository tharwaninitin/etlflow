package etlflow.steps.cloud

import ch.qos.logback.classic.{Level, Logger => LBLogger}
import etlflow.utils.Config
import io.circe.generic.auto._
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.regions.Region

trait CloudTestHelper {
  lazy val logger: Logger               = LoggerFactory.getLogger(getClass.getName)
  lazy val spark_logger: LBLogger       = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  lazy val spark_jetty_logger: LBLogger = LoggerFactory.getLogger("org.spark_project.jetty").asInstanceOf[LBLogger]
  spark_logger.setLevel(Level.WARN)
  spark_jetty_logger.setLevel(Level.WARN)

  lazy val config: Config              = io.circe.config.parser.decode[Config]().toOption.get
  lazy val gcs_bucket: String          = sys.env("GCS_BUCKET")
  lazy val s3_bucket: String           = sys.env("S3_BUCKET")
  lazy val s3_input_location: String   = sys.env("S3_INPUT_LOCATION")
  lazy val gcs_input_location: String  = sys.env("GCS_INPUT_LOCATION")
  lazy val gcs_output_location: String = sys.env("GCS_OUTPUT_LOCATION")
  lazy val pubsub_subscription:String  = sys.env("PUBSUB_SUBSCRIPTION")
  lazy val gcp_project_id: String      = sys.env("GCP_PROJECT_ID")
  lazy val s3_region: Region           = Region.AP_SOUTH_1

  val canonical_path: String    = new java.io.File(".").getCanonicalPath
  val file                      = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
  val file_csv                  = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings/ratings_1.csv"

  // val stream: Stream[Task, Unit] = for {
  //   s  <- Stream.resource(session)
  //   pq <- Stream.resource(s.prepare(insert))
  //   c  <- Stream.eval(pq.execute(record))
  // } yield ()
  //
  // stream.compile.drain
}
