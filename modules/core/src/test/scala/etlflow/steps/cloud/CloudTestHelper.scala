package etlflow.steps.cloud

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import cats.effect.Resource
import cats.implicits._
import ch.qos.logback.classic.{Level, Logger => LBLogger}
import etlflow.utils.Config
import org.slf4j.{Logger, LoggerFactory}
import natchez.Trace.Implicits.noop
import skunk._
import skunk.codec.all._
import skunk.implicits._
import software.amazon.awssdk.regions.Region
import zio.Task
import zio.interop.catz._
import scala.util.Try
import io.circe.generic.auto._

trait CloudTestHelper {
  lazy val logger: Logger               = LoggerFactory.getLogger(getClass.getName)
  lazy val spark_logger: LBLogger       = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  lazy val spark_jetty_logger: LBLogger = LoggerFactory.getLogger("org.spark_project.jetty").asInstanceOf[LBLogger]
  spark_logger.setLevel(Level.WARN)
  spark_jetty_logger.setLevel(Level.WARN)

  val config: Config              = io.circe.config.parser.decode[Config]().toOption.get
  val gcs_bucket: String          = sys.env("GCS_BUCKET")
  val s3_bucket: String           = sys.env("S3_BUCKET")
  val s3_input_location: String   = sys.env("S3_INPUT_LOCATION")
  val gcs_input_location: String  = sys.env("GCS_INPUT_LOCATION")
  val gcs_output_location: String = sys.env("GCS_OUTPUT_LOCATION")
  val pubsub_subscription:String  = sys.env("PUBSUB_SUBSCRIPTION")
  val gcp_project_id: String      = sys.env("GCP_PROJECT_ID")
  val s3_region: Region           = Region.AP_SOUTH_1

  val canonical_path: String    = new java.io.File(".").getCanonicalPath
  val file                      = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"

  case class QueryMetrics(start_time:LocalDateTime, email:String, query:String, duration:Double, status:String) {
    override def toString: String = s"$start_time $email $duration $status"
  }

  object QueryMetrics {
    val codec: Codec[QueryMetrics] = (timestamp, varchar, text, float8, varchar).imapN(QueryMetrics.apply)(QueryMetrics.unapply(_).get)
  }

  val session: Resource[Task, Session[Task]] = Session.single(
    host = sys.env.getOrElse("DB_HOST","localhost"),
    port = sys.env.getOrElse("DB_PORT","5432").toInt,
    user = config.dbLog.user,
    password = if (config.dbLog.password == "") None else Some(config.dbLog.password),
    database = "etlflow",
  )

  val createTableScript: Command[Void] = sql"""CREATE TABLE IF NOT EXISTS bqdump(
             start_time timestamp,
             email varchar(8000),
             query text,
             duration float8,
             status varchar(8000)
           )""".command

  val createTable: Task[Unit] = {
    session.use { s =>
      s.execute(createTableScript)
    }.as(())
  }

  def getDateTime(value: String): LocalDateTime = {
    val formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S'Z'")
    val formatter3 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")

    Try(LocalDateTime.parse(value, formatter1)).toOption match {
      case Some(value) => value
      case None => Try(LocalDateTime.parse(value, formatter2)).toOption match {
        case Some(value) => value
        case None => LocalDateTime.parse(value, formatter3)
      }
    }
  }

  val insert: Command[QueryMetrics] = sql"INSERT INTO BQDUMP VALUES (${QueryMetrics.codec})".command

  def getDuration(ldt1: LocalDateTime, ldt2: LocalDateTime): Double = Duration.between(ldt1, ldt2).toMillis/1000.0

  def insertDb(record: QueryMetrics): Task[Unit] = session.use { s =>
      s.prepare(insert).use { pc =>
        pc.execute(record)
      }
    }.as(())

  // val stream: Stream[Task, Unit] = for {
  //   s  <- Stream.resource(session)
  //   pq <- Stream.resource(s.prepare(insert))
  //   c  <- Stream.eval(pq.execute(record))
  // } yield ()
  //
  // stream.compile.drain
}
