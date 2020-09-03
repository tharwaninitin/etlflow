package etlflow.steps.cloud

import etlflow.etlsteps.CloudStoreSyncStep
import etlflow.utils.Location
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object CloudStoreSyncStepTestSuite extends DefaultRunnableSpec with CloudTestHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("CloudBucketSyncStep")(
      testM("Execute CloudSyncLOCALtoGCS step") {

        val step = CloudStoreSyncStep(
          name = "GCStoLOCALStep"
          , input_location = Location.LOCAL("modules/core/src/test/resources/input/movies/ratings/")
          , output_location = Location.GCS(gcs_output_location)
          , output_overwrite = true
          , parallelism = 3
          , chunk_size = 1000 * 1024
        )

        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute CloudSyncLOCALtoS3 step") {

        val step = CloudStoreSyncStep(
          name = "LOCALtoS3Step"
          , input_location = Location.LOCAL("modules/core/src/test/resources/input/movies/ratings/")
          , output_location = Location.S3(s3_input_location, s3_region)
          , output_overwrite = true
          , chunk_size = 1000 * 1024
        ).process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))

        assertM(step)(equalTo("ok"))
      },
      testM("Execute CloudSyncS3toLOCAL step") {

        val step = CloudStoreSyncStep(
          name = "S3toLOCALStep"
          , input_location = Location.S3(s3_input_location, s3_region)
          , output_location = Location.LOCAL("modules/core/src/test/resources/s3_output/")
          , output_overwrite = true
        ).process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))

        assertM(step)(equalTo("ok"))
      },
      testM("Execute CloudSyncGCStoLOCAL step") {

        val step = CloudStoreSyncStep(
          name = "GCStoLOCALStep"
          , input_location = Location.GCS(gcs_input_location)
          , output_location = Location.LOCAL("modules/core/src/test/resources/gcs_output/")
          , output_overwrite = true
          , parallelism = 3
        )

        assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute CloudSyncS3toGCS step") {

        val step = CloudStoreSyncStep(
          name = "S3toGCSStep"
          , input_location = Location.S3(s3_input_location, s3_region)
          , output_location = Location.GCS(gcs_output_location)
          , output_overwrite = true
        ).process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok"))

        assertM(step)(equalTo("ok"))
      },
    ) @@ TestAspect.sequential
}
