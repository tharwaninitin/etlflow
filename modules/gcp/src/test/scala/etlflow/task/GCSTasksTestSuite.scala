package etlflow.task

import etlflow.TestHelper
import etlflow.gcp.Location.{GCS, LOCAL}
import etlflow.log.LogEnv
import gcp4zio._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._
import scala.concurrent.duration._

object GCSTasksTestSuite extends TestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)

  val spec: ZSpec[environment.TestEnvironment with GCSEnv with LogEnv, Any] =
    suite("GCS Tasks")(
      testM("Execute GCSPut PARQUET task") {
        val task = GCSPutTask(
          name = "S3PutTask",
          bucket = gcsBucket,
          prefix = "temp/ratings.parquet",
          file = filePathParquet
        ).executeZio
        assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSPut CSV task") {
        val task = GCSPutTask(
          name = "S3PutTask",
          bucket = gcsBucket,
          prefix = "temp/ratings.csv",
          file = filePathCsv
        ).executeZio
        assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSSensor task") {
        val task = GCSSensorTask(
          name = "GCSKeySensor",
          bucket = gcsBucket,
          prefix = "temp/ratings.parquet",
          retry = 10,
          spaced = 5.second
        ).executeZio
        assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSCopy task GCS to GCS") {
        val task = GCSCopyTask(
          name = "CopyTask",
          input = GCS(gcsBucket, "temp"),
          inputRecursive = true,
          output = GCS(gcsBucket, "temp2"),
          parallelism = 2
        ).executeZio
        assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSCopy task LOCAL to GCS") {
        val task = GCSCopyTask(
          name = "CopyTask",
          input = LOCAL("/local/path"),
          inputRecursive = true,
          output = GCS(gcsBucket, "temp2"),
          parallelism = 2
        ).executeZio
        assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
