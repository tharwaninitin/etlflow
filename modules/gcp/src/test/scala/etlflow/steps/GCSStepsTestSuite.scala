package etlflow.steps

import etlflow.TestHelper
import etlflow.etlsteps.{GCSCopyStep, GCSPutStep, GCSSensorStep}
import etlflow.gcp.Location.{GCS, LOCAL}
import etlflow.log.LogEnv
import gcp4zio._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._
import scala.concurrent.duration._

object GCSStepsTestSuite extends TestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)

  val spec: ZSpec[environment.TestEnvironment with GCSEnv with LogEnv, Any] =
    suite("GCS Steps")(
      testM("Execute GCSPut PARQUET step") {
        val step = GCSPutStep(
          name = "S3PutStep",
          bucket = gcsBucket,
          prefix = "temp/ratings.parquet",
          file = filePathParquet
        ).execute
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSPut CSV step") {
        val step = GCSPutStep(
          name = "S3PutStep",
          bucket = gcsBucket,
          prefix = "temp/ratings.csv",
          file = filePathCsv
        ).execute
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSSensor step") {
        val step = GCSSensorStep(
          name = "GCSKeySensor",
          bucket = gcsBucket,
          prefix = "temp/ratings.parquet",
          retry = 10,
          spaced = 5.second
        ).execute
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSCopy step GCS to GCS") {
        val step = GCSCopyStep(
          name = "CopyStep",
          input = GCS(gcsBucket, "temp"),
          inputRecursive = true,
          output = GCS(gcsBucket, "temp2"),
          parallelism = 2
        ).execute
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSCopy step LOCAL to GCS") {
        val step = GCSCopyStep(
          name = "CopyStep",
          input = LOCAL("/local/path"),
          inputRecursive = true,
          output = GCS(gcsBucket, "temp2"),
          parallelism = 2
        ).execute
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
