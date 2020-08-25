package etlflow.steps

import etlflow.etlsteps.CloudStoreSyncStep
import etlflow.utils.{Environment, Location}
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object CloudStoreSyncStepTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
        testM("Execute CloudSyncStep step") {
          val step = CloudStoreSyncStep(
            name             = "GCStoLOCALStep"
            ,input_location  = Location.GCS(sys.env("GCS_INPUT_LOCATION"))
            ,output_location = Location.LOCAL("modules/core/src/test/resources/test_output/")
            ,parallelism     = 3
          )
          assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        }
    )
}


