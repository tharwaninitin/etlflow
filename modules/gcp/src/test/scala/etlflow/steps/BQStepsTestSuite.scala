package etlflow.steps

import com.google.cloud.bigquery.Schema
import etlflow.TestHelper
import etlflow.etlsteps.{BQExportStep, BQLoadStep}
import etlflow.log.LogEnv
import gcp4zio.BQInputType.{CSV, PARQUET}
import gcp4zio._
import utils.Encoder
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object BQStepsTestSuite extends TestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)

  // STEP 1: Define step
  private val inputFileParquet = s"gs://$gcsBucket/temp/ratings.parquet"
  private val inputFileCsv     = s"gs://$gcsBucket/temp/ratings.csv"
  private val bqExportDestPath = s"gs://$gcsBucket/temp/etlflow/"
  private val outputTable      = "ratings"
  private val outputDataset    = "dev"

  val spec: ZSpec[environment.TestEnvironment with BQEnv with LogEnv, Any] = suite("BQ Steps")(
    testM("Execute BQLoad PARQUET step") {
      val step = BQLoadStep(
        name = "LoadRatingBQ",
        inputLocation = Left(inputFileParquet),
        inputType = PARQUET,
        outputProject = sys.env.get("GCP_PROJECT_ID"),
        outputDataset = outputDataset,
        outputTable = outputTable
      ).execute
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute BQLoad CSV step") {
      val schema: Option[Schema] = Encoder[RatingCSV]
      val step = BQLoadStep(
        name = "LoadRatingCSV",
        inputLocation = Left(inputFileCsv),
        inputType = CSV(),
        outputProject = sys.env.get("GCP_PROJECT_ID"),
        outputDataset = outputDataset,
        outputTable = outputTable,
        schema = schema
      ).execute
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute BQExport CSV step") {
      val step = BQExportStep(
        name = "ExportRatingBQPARQUETCSV",
        sourceProject = sys.env.get("GCP_PROJECT_ID"),
        sourceDataset = outputDataset,
        sourceTable = outputTable,
        destinationPath = bqExportDestPath,
        destinationFileName = Some("sample.csv"),
        destinationFormat = BQInputType.CSV(",")
      ).execute
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute BQExport PARQUET step") {
      val step = BQExportStep(
        name = "ExportRatingBQPARQUET",
        sourceProject = sys.env.get("GCP_PROJECT_ID"),
        sourceDataset = outputDataset,
        sourceTable = outputTable,
        destinationPath = bqExportDestPath,
        destinationFileName = Some("sample.parquet"),
        destinationFormat = BQInputType.PARQUET,
        destinationCompressionType = "snappy"
      ).execute
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
  ) @@ TestAspect.sequential
}
