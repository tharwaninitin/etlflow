package etlflow.steps

import etlflow.etlsteps.CloudSyncStep
import etlflow.utils.Environment
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object CloudSyncStepTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
        testM("Execute CloudSyncStep step") {
          val step = CloudSyncStep(
            name        = "CloudSyncStep",
            creds       = Some(Environment.GCP(sys.env("GOOGLE_APPLICATION_CREDENTIALS"))),
            inputPath   = sys.env("GCS_INPUT_PATH"),
            outputPath  = sys.env("GCS_OUTPUT_PATH")
          )
          assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        }
    )
}


