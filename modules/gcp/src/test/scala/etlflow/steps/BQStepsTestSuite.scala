package etlflow.steps

import com.google.cloud.bigquery.Schema
import etlflow.TestHelper
import etlflow.etlsteps.{BQExportStep, BQLoadStep}
import gcp4zio.BQInputType.{CSV, PARQUET}
import gcp4zio._
import utils.Encoder
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object BQStepsTestSuite extends TestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)

  // STEP 1: Define step
  val input_file_parquet  = s"gs://$gcs_bucket/temp/ratings.parquet"
  val input_file_csv      = s"gs://$gcs_bucket/temp/ratings.csv"
  val bq_export_dest_path = s"gs://$gcs_bucket/temp/etlflow/"
  val output_table        = "ratings"
  val output_dataset      = "dev"

  val spec: ZSpec[environment.TestEnvironment with BQEnv, Any] = suite("BQ Steps")(
    testM("Execute BQLoad PARQUET step") {
      val step = BQLoadStep(
        name = "LoadRatingBQ",
        input_location = Left(input_file_parquet),
        input_type = PARQUET,
        output_project = sys.env.get("GCP_PROJECT_ID"),
        output_dataset = output_dataset,
        output_table = output_table
      ).process
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute BQLoad CSV step") {
      val schema: Option[Schema] = Encoder[RatingCSV]
      val step = BQLoadStep(
        name = "LoadRatingCSV",
        input_location = Left(input_file_csv),
        input_type = CSV(),
        output_project = sys.env.get("GCP_PROJECT_ID"),
        output_dataset = output_dataset,
        output_table = output_table,
        schema = schema
      ).process
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute BQExport CSV step") {
      val step = BQExportStep(
        name = "ExportRatingBQPARQUETCSV",
        source_project = sys.env.get("GCP_PROJECT_ID"),
        source_dataset = output_dataset,
        source_table = output_table,
        destination_path = bq_export_dest_path,
        destination_file_name = Some("sample.csv"),
        destination_format = BQInputType.CSV(",")
      ).process
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    testM("Execute BQExport PARQUET step") {
      val step = BQExportStep(
        name = "ExportRatingBQPARQUET",
        source_project = sys.env.get("GCP_PROJECT_ID"),
        source_dataset = output_dataset,
        source_table = output_table,
        destination_path = bq_export_dest_path,
        destination_file_name = Some("sample.parquet"),
        destination_format = BQInputType.PARQUET,
        destination_compression_type = "snappy"
      ).process
      assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
  ) @@ TestAspect.sequential
}
