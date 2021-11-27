package etlflow.steps

import etlflow.GcpTestHelper
import etlflow.etlsteps.{GCSCopyStep, GCSPutStep, GCSSensorStep}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._
import scala.concurrent.duration._

object GCSStepsTestSuite extends DefaultRunnableSpec with GcpTestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("GCS Steps")(
      testM("Execute GCSPut PARQUET step") {
        val step = GCSPutStep(
          name = "S3PutStep",
          bucket = gcs_bucket,
          key = "temp/ratings.parquet",
          file = file
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSPut CSV step") {
        val step = GCSPutStep(
          name = "S3PutStep",
          bucket = gcs_bucket,
          key = "temp/ratings.csv",
          file = file_csv
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSSensor step") {
        val step = GCSSensorStep(
          name = "GCSKeySensor",
          bucket = gcs_bucket,
          prefix = "temp",
          key = "ratings.parquet",
          retry = 10,
          spaced = 5.second
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSCopy step") {
        val step = GCSCopyStep(
          name = "CopyStep",
          src_bucket = gcs_bucket,
          src_prefix = "temp",
          target_bucket = gcs_bucket,
          target_prefix = "temp2",
          parallelism = 2
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
