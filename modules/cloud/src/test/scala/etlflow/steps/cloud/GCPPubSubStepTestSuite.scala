package etlflow.steps.cloud

import com.permutive.pubsub.consumer.decoder.MessageDecoder
import etlflow.etlsteps.GooglePubSubSourceStep
import zio.test.Assertion._
import zio.test._
import zio.{Task, ZIO}

object GCPPubSubStepTestSuite extends DefaultRunnableSpec with CloudTestHelper with DbHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
        testM("Execute PubSub step") {

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
