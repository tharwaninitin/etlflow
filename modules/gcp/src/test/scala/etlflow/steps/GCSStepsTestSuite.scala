package etlflow.steps

import etlflow.TestHelper
import etlflow.etltask.{GCSCopyTask, GCSPutTask, GCSSensorTask}
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
        val step = GCSPutTask(
          name = "S3PutStep",
          bucket = gcsBucket,
          prefix = "temp/ratings.parquet",
          file = filePathParquet
        ).executeZio
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSPut CSV step") {
        val step = GCSPutTask(
          name = "S3PutStep",
          bucket = gcsBucket,
          prefix = "temp/ratings.csv",
          file = filePathCsv
        ).executeZio
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSSensor step") {
        val step = GCSSensorTask(
          name = "GCSKeySensor",
          bucket = gcsBucket,
          prefix = "temp/ratings.parquet",
          retry = 10,
          spaced = 5.second
        ).executeZio
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSCopy step GCS to GCS") {
        val step = GCSCopyTask(
          name = "CopyStep",
          input = GCS(gcsBucket, "temp"),
          inputRecursive = true,
          output = GCS(gcsBucket, "temp2"),
          parallelism = 2
        ).executeZio
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSCopy step LOCAL to GCS") {
        val step = GCSCopyTask(
          name = "CopyStep",
          input = LOCAL("/local/path"),
          inputRecursive = true,
          output = GCS(gcsBucket, "temp2"),
          parallelism = 2
        ).executeZio
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
