package etlflow.task

import com.google.cloud.bigquery.Schema
import etlflow.TestHelper
import etlflow.log.LogEnv
import gcp4zio.BQInputType.{CSV, PARQUET}
import gcp4zio._
import utils.Encoder
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

  val spec: ZSpec[environment.TestEnvironment with BQEnv with LogEnv, Any] = suite("BQ Tasks")(
    testM("Execute BQLoad PARQUET task") {
      val task = BQLoadTask(
        name = "LoadRatingBQ",
        inputLocation = Left(inputFileParquet),
        inputType = PARQUET,
        outputProject = sys.env.get("GCP_PROJECT_ID"),
        outputDataset = outputDataset,
        outputTable = outputTable
      ).executeZio
      assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute BQLoad CSV task") {
      val schema: Option[Schema] = Encoder[RatingCSV]
      val task = BQLoadTask(
        name = "LoadRatingCSV",
        inputLocation = Left(inputFileCsv),
        inputType = CSV(),
        outputProject = sys.env.get("GCP_PROJECT_ID"),
        outputDataset = outputDataset,
        outputTable = outputTable,
        schema = schema
      ).executeZio
      assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute BQExport CSV task") {
      val task = BQExportTask(
        name = "ExportRatingBQPARQUETCSV",
        sourceProject = sys.env.get("GCP_PROJECT_ID"),
        sourceDataset = outputDataset,
        sourceTable = outputTable,
        destinationPath = bqExportDestPath,
        destinationFileName = Some("sample.csv"),
        destinationFormat = BQInputType.CSV(",")
      ).executeZio
      assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute BQExport PARQUET task") {
      val task = BQExportTask(
        name = "ExportRatingBQPARQUET",
        sourceProject = sys.env.get("GCP_PROJECT_ID"),
        sourceDataset = outputDataset,
        sourceTable = outputTable,
        destinationPath = bqExportDestPath,
        destinationFileName = Some("sample.parquet"),
        destinationFormat = BQInputType.PARQUET,
        destinationCompressionType = "snappy"
      ).executeZio
      assertM(task.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
  ) @@ TestAspect.sequential
}
