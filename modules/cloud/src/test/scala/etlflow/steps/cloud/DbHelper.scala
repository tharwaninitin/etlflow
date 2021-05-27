//package etlflow.steps.cloud
//
//import java.time.format.DateTimeFormatter
//import java.time.{Duration, LocalDateTime}
//import cats.effect.Resource
//import io.circe.Json
//import io.circe.optics.JsonPath.root
//import io.circe.parser.parse
//import etlflow.utils.Configuration
//import natchez.Trace.Implicits.noop
//import skunk._
//import skunk.codec.all._
//import cats.implicits._
//import skunk.implicits._
//import zio.interop.catz._
//import zio.Task
//import scala.util.Try
//
//trait DbHelper extends Configuration {
//
//  case class QueryMetrics(start_time:LocalDateTime, email:String, query:String, duration:Double, status:String) {
//    override def toString: String = s"$start_time $email $duration $status"
//  }
//
//  object QueryMetrics {
//    val codec: Codec[QueryMetrics] = (timestamp, varchar, text, float8, varchar).imapN(QueryMetrics.apply)(QueryMetrics.unapply(_).get)
//  }
//
//  val session: Resource[Task, Session[Task]] = Session.single(
//    host = sys.env.getOrElse("DB_HOST","localhost"),
//    port = 5432,
//    user = config.dbLog.user,
//    password = Some(config.dbLog.password),
//    database = "etlflow",
//  )
//
//  val createTableScript: Command[Void] = sql"""CREATE TABLE IF NOT EXISTS bqdump(
//             start_time timestamp,
//             email varchar(8000),
//             query text,
//             duration float8,
//             status varchar(8000)
//           )""".command
//
//  val createTable: Task[Unit] = {
//    session.use { s =>
//      s.execute(createTableScript)
//    }.unit
//  }
//
//  def getDateTime(value: String): LocalDateTime = {
//    val formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
//    val formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.S'Z'")
//    val formatter3 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
//
//    Try(LocalDateTime.parse(value, formatter1)).toOption match {
//      case Some(value) => value
//      case None => Try(LocalDateTime.parse(value, formatter2)).toOption match {
//        case Some(value) => value
//        case None => LocalDateTime.parse(value, formatter3)
//      }
//    }
//  }
//
//  val insert: Command[QueryMetrics] = sql"INSERT INTO BQDUMP VALUES (${QueryMetrics.codec})".command
//
//  def getDuration(ldt1: LocalDateTime, ldt2: LocalDateTime): Double = Duration.between(ldt1, ldt2).toMillis/1000.0
//
//  def insertDb(record: QueryMetrics): Task[Unit] = session.use { s =>
//    s.prepare(insert).use { pc =>
//      pc.execute(record)
//    }
//  }.unit
//
//  def jsonParser(message: String): Either[Throwable,QueryMetrics] = {
//    val json = parse(message).getOrElse(Json.Null)
//    val message_type = root.protoPayload.methodName.string.getOption(json)
//    message_type match {
//      case Some("jobservice.jobcompleted") =>
//        Try{
//          val principalEmail = root.protoPayload.authenticationInfo.principalEmail.string.getOption(json)
//          val raw_query = root.protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.query.query.string.getOption(json)
//          val error = root.protoPayload.serviceData.jobCompletedEvent.job.jobStatus.error.string.getOption(json)
//          val status = root.protoPayload.serviceData.jobCompletedEvent.job.jobStatus.state.string.getOption(json)
//          val query_status = if (error.isEmpty) status else error
//          val query = raw_query.map(_.trim.replaceAll("\n", "").trim.replaceAll("\t", ""))
//
//          val startTime = root.protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.startTime.string.getOption(json).getOrElse("")
//          val endTime = root.protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.endTime.string.getOption(json).getOrElse("")
//
//          val startTimeStamp  = getDateTime(startTime)
//          val endTimeStamp    = getDateTime(endTime)
//          val duration        = getDuration(startTimeStamp,endTimeStamp)
//
//          QueryMetrics(startTimeStamp,principalEmail.get,query.get,duration,query_status.get)
//        }.toEither
//      case msg =>
//        Left(new RuntimeException(s"Unknown message type ${msg.getOrElse("")} "))
//    }
//  }
//
//}
