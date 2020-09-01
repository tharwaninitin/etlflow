package etlflow.steps.cloud

import etlflow.etlsteps.{CloudStoreStep, CloudStoreToPostgresStep}
import etlflow.utils.Location
import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse
import zio.test.Assertion._
import zio.test._
import zio.{Task, ZIO}
import scala.util.Try
import fs2.text
import zio.interop.catz.implicits._
import scala.concurrent.duration._

object CloudStoreTestSuite extends DefaultRunnableSpec with CloudTestHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
        testM("Execute CloudStore step") {

          def jsonParser(message: String): Either[Throwable,QueryMetrics] = {
            val json = parse(message).getOrElse(Json.Null)
            val message_type = root.protoPayload.methodName.string.getOption(json)
            message_type match {
              case Some("jobservice.jobcompleted") =>
                Try{
                  val principalEmail = root.protoPayload.authenticationInfo.principalEmail.string.getOption(json)
                  val raw_query = root.protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.query.query.string.getOption(json)
                  val error = root.protoPayload.serviceData.jobCompletedEvent.job.jobStatus.error.string.getOption(json)
                  val status = root.protoPayload.serviceData.jobCompletedEvent.job.jobStatus.state.string.getOption(json)
                  val query_status = if (error.isEmpty) status else error
                  val query = raw_query.map(_.trim.replaceAll("\n", "").trim.replaceAll("\t", ""))

                  val startTime = root.protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.startTime.string.getOption(json).getOrElse("")
                  val endTime = root.protoPayload.serviceData.jobCompletedEvent.job.jobStatistics.endTime.string.getOption(json).getOrElse("")

                  val startTimeStamp  = getDateTime(startTime)
                  val endTimeStamp    = getDateTime(endTime)
                  val duration        = getDuration(startTimeStamp,endTimeStamp)

                  QueryMetrics(startTimeStamp,principalEmail.get,query.get,duration,query_status.get)
                }.toEither
              case msg =>
                Left(new RuntimeException(s"Unknown message type ${msg.getOrElse("")} "))
            }
          }

          val step1 = CloudStoreStep[QueryMetrics](
            name              = "GCSBigQueryLogsToPostgresStep"
            ,input_location   = Location.GCS(gcs_input_location)
            ,transformation   = _.through(text.utf8Decode).through(text.lines).map(jsonParser)
            ,error_handler    = ex => Task.succeed(println(s"Got error ${ex.getMessage}"))
            ,success_handler  = message => insertDb(message)
          ).process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))

          val step2 = CloudStoreToPostgresStep[QueryMetrics](
            name            = "GCSBigQueryLogsToPostgresStep"
            ,input_location = Location.GCS(gcs_input_location)
            ,transformation = _.through(text.utf8Decode).through(text.lines).map(jsonParser)
            ,error_handler  = ex => Task.succeed(println(s"Got error ${ex.getMessage}"))
            ,pg_session     = session
            ,pg_command     = insert
          ).process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))

          assertM(createTable *> step2)(equalTo("ok"))
        }
    )
}
