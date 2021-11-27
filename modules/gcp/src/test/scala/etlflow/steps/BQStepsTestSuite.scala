package etlflow.steps

import com.google.cloud.bigquery.Schema
import etlflow.GcpTestHelper
import etlflow.etlsteps.{BQExportStep, BQLoadStep}
import etlflow.gcp.BQInputType.{CSV, PARQUET}
import etlflow.gcp.{BQInputType, getBqSchema}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object BQStepsTestSuite extends DefaultRunnableSpec with GcpTestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)

  // STEP 1: Define step
  val input_file_parquet = s"gs://$gcs_bucket/temp/ratings.parquet"
  val input_file_csv = s"gs://$gcs_bucket/temp/ratings.csv"
  val bq_export_dest_path = s"gs://$gcs_bucket/temp/etlflow/"

  val output_table = "ratings"
  val output_dataset = "dev"

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("BQ Steps")(
      testM("Execute BQLoad PARQUET step") {
        val step = BQLoadStep(
          name = "LoadRatingBQ",
          input_location = Left(input_file_parquet),
          input_type = PARQUET,
          output_project = sys.env.get("GCP_PROJECT_ID"),
          output_dataset = output_dataset,
          output_table = output_table
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute BQLoad CSV step") {
        val schema: Option[Schema] = getBqSchema[RatingCSV]
        val step = BQLoadStep(
          name = "LoadRatingCSV",
          input_location = Left(input_file_csv),
          input_type = CSV(),
          output_project = sys.env.get("GCP_PROJECT_ID"),
          output_dataset = output_dataset,
          output_table = output_table,
          schema = schema
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute BQExport CSV step") {
        val step = BQExportStep(
          name = "ExportRatingBQPARQUETCSV"
          , source_project = sys.env.get("GCP_PROJECT_ID")
          , source_dataset = output_dataset
          , source_table = output_table
          , destination_path = bq_export_dest_path
          , destination_file_name = Some("sample.csv")
          , destination_format = BQInputType.CSV(",")
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute BQExport PARQUET step") {
        val step = BQExportStep(
          name = "ExportRatingBQPARQUET"
          , source_project = sys.env.get("GCP_PROJECT_ID")
          , source_dataset = output_dataset
          , source_table = output_table
          , destination_path = bq_export_dest_path
          , destination_file_name = Some("sample.parquet")
          , destination_format = BQInputType.PARQUET
          , destination_compression_type = "snappy"
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential
}