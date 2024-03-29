package etlflow.task

import etlflow.TestHelper
import etlflow.audit.Audit
import etlflow.gcp.Location.{GCS => GCSLocation, LOCAL}
import gcp4zio.gcs._
import zio.test.Assertion.equalTo
import zio.test._
import zio.{Clock, ZIO}
import scala.concurrent.duration._

object GCSTasksTestSuite extends TestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)

  val spec: Spec[Clock with GCS with Audit, Any] =
    suite("GCS Tasks")(
      test("Execute GCSPut PARQUET task") {
        val task = GCSPutTask(
          name = "S3PutTask",
          bucket = gcsBucket,
          prefix = "temp/ratings.parquet",
          file = filePathParquet
        ).toZIO
        assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute GCSPut CSV task") {
        val task = GCSPutTask(
          name = "S3PutTask",
          bucket = gcsBucket,
          prefix = "temp/ratings.csv",
          file = filePathCsv
        ).toZIO
        assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute GCSSensor task") {
        val task = GCSSensorTask(
          name = "GCSKeySensor",
          bucket = gcsBucket,
          prefix = "temp/ratings.parquet",
          retry = 10,
          spaced = 5.second
        ).toZIO
        assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute GCSCopy task GCS to GCS") {
        val task = GCSCopyTask(
          name = "CopyTask",
          input = GCSLocation(gcsBucket, "temp"),
          inputRecursive = true,
          output = GCSLocation(gcsBucket, "temp2"),
          parallelism = 2
        ).toZIO
        assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute GCSCopy task LOCAL to GCS") {
        val task = GCSCopyTask(
          name = "CopyTask",
          input = LOCAL("/local/path"),
          inputRecursive = true,
          output = GCSLocation(gcsBucket, "temp2"),
          parallelism = 2
        ).toZIO
        assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}
