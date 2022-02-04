package etlflow.steps

import etlflow.TestHelper
import etlflow.etlsteps.{GCSCopyStep, GCSPutStep, GCSSensorStep}
import etlflow.gcp.Location.{GCS, LOCAL}
import gcp4zio._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._
import scala.concurrent.duration._

object GCSStepsTestSuite extends TestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)

  val spec: ZSpec[environment.TestEnvironment with GCSEnv, Any] =
    suite("GCS Steps")(
      testM("Execute GCSPut PARQUET step") {
        val step = GCSPutStep(
          name = "S3PutStep",
          bucket = gcs_bucket,
          key = "temp/ratings.parquet",
          file = file_path_parquet
        ).process
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSPut CSV step") {
        val step = GCSPutStep(
          name = "S3PutStep",
          bucket = gcs_bucket,
          key = "temp/ratings.csv",
          file = file_path_csv
        ).process
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSSensor step") {
        val step = GCSSensorStep(
          name = "GCSKeySensor",
          bucket = gcs_bucket,
          prefix = "temp",
          key = "ratings.parquet",
          retry = 10,
          spaced = 5.second
        ).process
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSCopy step GCS to GCS") {
        val step = GCSCopyStep(
          name = "CopyStep",
          input = GCS(gcs_bucket, "temp"),
          output = GCS(gcs_bucket, "temp2"),
          parallelism = 2
        ).process
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSCopy step LOCAL to GCS") {
        val step = GCSCopyStep(
          name = "CopyStep",
          input = LOCAL("/local/path"),
          output = GCS(gcs_bucket, "temp2"),
          parallelism = 2
        ).process
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
