package etlflow.steps.cloud

import etlflow.etlsteps.{CloudStoreStep, CloudStoreToPostgresStep}
import etlflow.utils.Location
import zio.test.Assertion._
import zio.test._
import zio.{Task, ZIO}
import fs2.text

object CloudStoreTestSuite extends DefaultRunnableSpec with CloudTestHelper with DbHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
        testM("Execute CloudStore step") {

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
