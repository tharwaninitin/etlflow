package etlflow.steps.cloud

import com.permutive.pubsub.consumer.decoder.MessageDecoder
import etlflow.etlsteps.GooglePubSubSourceStep
import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse
import zio.test.Assertion._
import zio.test._
import zio.{Task, ZIO}
import scala.util.Try

object GCPPubSubStepTestSuite extends DefaultRunnableSpec with CloudTestHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
        testM("Execute PubSub step") {

          def jsonParser(message: String): Either[Throwable,QueryMetrics] = {
            val json = parse(message).getOrElse(Json.Null)
            val message_type = root.protoPayload.methodName.string.getOption(json)
            message_type match {
              case Some("jobservice.getqueryresults") =>
                Try{
                  val principalEmail = root.protoPayload.authenticationInfo.principalEmail.string.getOption(json).get
                  val raw_query = root.protoPayload.serviceData.jobGetQueryResultsResponse.job.jobConfiguration.query.query.string.getOption(json)
                  val error = root.protoPayload.serviceData.jobGetQueryResultsResponse.job.jobStatus.error.string.getOption(json)
                  val status = root.protoPayload.serviceData.jobGetQueryResultsResponse.job.jobStatus.state.string.getOption(json)
                  val query_status = (if (error.isEmpty) status else error).get
                  val startTime = root.protoPayload.serviceData.jobGetQueryResultsResponse.job.jobStatistics.startTime.string.getOption(json).getOrElse("")
                  val endTime = root.protoPayload.serviceData.jobGetQueryResultsResponse.job.jobStatistics.endTime.string.getOption(json).getOrElse("")
                  val query = raw_query.get.trim.replaceAll("\n", "").trim.replaceAll("\t", "")

                  val startTimeStamp  = getDateTime(startTime)
                  val endTimeStamp    = getDateTime(endTime)
                  val duration        = getDuration(startTimeStamp,endTimeStamp)

                  QueryMetrics(startTimeStamp,principalEmail,query,duration,query_status)
                }.toEither
              case msg =>
                Left(new RuntimeException(s"Unknown message type ${msg.getOrElse("")} "))
            }
          }

          implicit val decoder: MessageDecoder[QueryMetrics] = (bytes: Array[Byte]) => jsonParser(new String(bytes))

          val step = GooglePubSubSourceStep[QueryMetrics](
            name              = "PubSubConsumerBigQueryLogsToPostgresStep"
            ,subscription     = pubsub_subscription
            ,project_id       = gcp_project_id
            ,success_handler  = message => insertDb(message.value) *> Task(println(message.value.toString)) *> message.ack
            ,limit            = Some(10)
          ).process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))

          assertM(createTable *> step)(equalTo("ok"))
        }
    )
}
