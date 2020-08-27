package etlflow.steps

import etlflow.TestSuiteHelper
import etlflow.etlsteps.CloudStoreSyncStep
import etlflow.utils.Location
import software.amazon.awssdk.regions.Region
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object CloudStoreSyncStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      suite("CloudStoreSyncStep Step")(
        testM("Execute CloudSyncS3Step step") {

          val step = CloudStoreSyncStep(
            name = "S3toLOCALStep"
            , input_location = Location.S3(sys.env("S3_INPUT_LOCATION"), s3_region)
            , output_location = Location.LOCAL("modules/core/src/test/resources/s3_output/")
            , output_overwrite = true
          ).process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))

          assertM(step)(equalTo("ok"))
        },
        testM("Execute CloudSyncGCSStep step") {

          val step = CloudStoreSyncStep(
            name = "GCStoLOCALStep"
            , input_location = Location.GCS(sys.env("GCS_INPUT_LOCATION"))
            , output_location = Location.LOCAL("modules/core/src/test/resources/gcs_output/")
            , output_overwrite = true
            , parallelism = 3
          )

          assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        }
      ) @@ TestAspect.sequential
    )
}


