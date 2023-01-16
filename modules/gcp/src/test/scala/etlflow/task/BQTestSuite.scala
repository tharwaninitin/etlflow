package etlflow.task

import com.google.cloud.bigquery.{FieldValueList, Schema}
import etlflow.TestHelper
import etlflow.audit.Audit
import gcp4zio.bq.FileType.{CSV, PARQUET}
import gcp4zio.bq._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object BQTestSuite extends TestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)

  // TASK 1: Define task
  private val inputFileParquet = s"gs://$gcsBucket/temp/ratings.parquet"
  private val inputFileCsv     = s"gs://$gcsBucket/temp/ratings.csv"
  private val bqExportDestPath = s"gs://$gcsBucket/temp/etlflow/"
  private val outputTable      = "ratings"
  private val outputDataset    = "dev"

  val spec: Spec[BQ with Audit, Any] = suite("BQ Tasks")(
    test("Execute BQLoad PARQUET task") {
      val task = BQLoadTask(
        name = "LoadRatingBQ",
        inputLocation = Left(inputFileParquet),
        inputType = PARQUET,
        outputProject = sys.env.get("GCP_PROJECT_ID"),
        outputDataset = outputDataset,
        outputTable = outputTable
      ).toZIO
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    test("Execute BQLoad CSV task") {
      val schema: Option[Schema] = Encoder[RatingCSV]
      val task = BQLoadTask(
        name = "LoadRatingCSV",
        inputLocation = Left(inputFileCsv),
        inputType = CSV(),
        outputProject = sys.env.get("GCP_PROJECT_ID"),
        outputDataset = outputDataset,
        outputTable = outputTable,
        schema = schema
      ).toZIO
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    test("Execute BQExport CSV task") {
      val task = BQExportTask(
        name = "ExportRatingBQPARQUETCSV",
        sourceProject = sys.env.get("GCP_PROJECT_ID"),
        sourceDataset = outputDataset,
        sourceTable = outputTable,
        destinationPath = bqExportDestPath,
        destinationFileName = Some("sample.csv"),
        destinationFormat = CSV(",")
      ).toZIO
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    test("Execute BQExport PARQUET task") {
      val task = BQExportTask(
        name = "ExportRatingBQPARQUET",
        sourceProject = sys.env.get("GCP_PROJECT_ID"),
        sourceDataset = outputDataset,
        sourceTable = outputTable,
        destinationPath = bqExportDestPath,
        destinationFileName = Some("sample.parquet"),
        destinationFormat = PARQUET,
        destinationCompressionType = "snappy"
      ).toZIO
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    test("Execute BQSensorTask") {
      def sensor(rows: Iterable[FieldValueList]): Boolean =
        rows.headOption.fold(-1)(res => res.get(0).getLongValue.toInt) == 0
      val task = BQSensorTask(
        name = "PollTask",
        query = s"SELECT count(*) FROM `$bqDataset.$bqTable`",
        sensor
      ).toZIO
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
  ) @@ TestAspect.sequential
}
