package etlflow.steps.gcp

import etlflow.etlsteps.{BQExportStep, BQLoadStep, GCSPutStep, GCSSensorStep}
import etlflow.gcp.BQInputType
import etlflow.gcp.BQInputType.{CSV, PARQUET}
import zio.ZIO
import zio.test.Assertion._
import zio.test.{DefaultRunnableSpec, _}

import scala.concurrent.duration._

object GCPStepsTestSuite extends DefaultRunnableSpec with GcpTestHelper {
  case class RatingCSV(userId: Long, movieId: Long, rating: Double, timestamp: Long)
  // STEP 1: Define step
  val input_path = s"gs://$gcs_bucket/temp/ratings.parquet"
  val input_path_csv = s"gs://$gcs_bucket/temp/ratings.csv"
  val bq_export_dest_path = s"gs://$gcs_bucket/temp/etlflow/"

  val output_table = "ratings"
  val output_dataset = "dev"

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("GCP Steps")(
      testM("Execute GCSPut PARQUET step") {
        val step = GCSPutStep(
          name    = "S3PutStep",
          bucket  = gcs_bucket,
          key     = "temp/ratings.parquet",
          file    = file
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSPut CSV step") {
        val step = GCSPutStep(
          name    = "S3PutStep",
          bucket  = gcs_bucket,
          key     = "temp/ratings.csv",
          file    = file_csv
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute GCSSensor step") {
        val step = GCSSensorStep(
          name    = "GCSKeySensor",
          bucket  = gcs_bucket,
          prefix  = "temp",
          key     = "ratings.parquet",
          retry   = 10,
          spaced  = 5.second
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute BQLoad PARQUET step") {
        val step = BQLoadStep(
          name           = "LoadRatingBQ",
          input_location = Left(input_path),
          input_type     = PARQUET,
          output_project = sys.env.get("GCP_PROJECT_ID"),
          output_dataset = output_dataset,
          output_table   = output_table
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute BQLoad CSV step") {
        val step = BQLoadStep[RatingCSV](
          name           = "LoadRatingCSV",
          input_location = Left(input_path_csv),
          input_type     = CSV(),
          output_project = sys.env.get("GCP_PROJECT_ID"),
          output_dataset = output_dataset,
          output_table   = output_table
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute BQ Export CSV step") {
        val step = BQExportStep(
          name           = "ExportRatingBQPARQUETCSV"
          , source_project = sys.env.get("GCP_PROJECT_ID")
          , source_dataset =  output_dataset
          , source_table = output_table
          , destination_path = bq_export_dest_path
          , destination_format = BQInputType.CSV(",")
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute BQ Export PARQUET step") {
        val step = BQExportStep(
          name           = "ExportRatingBQPARQUET"
          , source_project = sys.env.get("GCP_PROJECT_ID")
          , source_dataset =  output_dataset
          , source_table = output_table
          , destination_path = bq_export_dest_path
          , destination_file_name = Some("sample.parquet")
          , destination_format = BQInputType.PARQUET
          , destination_compression_type = "snappy"
        )
        assertM(step.process(()).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential

  // STEP 3: Verify Results
  //  private implicit val spark: SparkSession = SparkManager.createSparkSession()
  //
  //  val raw: Dataset[Rating] = ReadApi.LoadDS[Rating](Seq(input_path), PARQUET)(spark)
  //  val Row(sum_ratings: Double, count_ratings: Long) = raw.selectExpr("sum(rating)","count(*)").first()
  //  val query: String = s"SELECT count(*) as count_ratings ,sum(rating) sum_ratings FROM $output_dataset.$output_table"
  //  val env = BQ.live()
  //  val result: Iterable[FieldValueList] = runtime.unsafeRun(BQService.getDataFromBQ(query).provideLayer(env))
  //  val count_records_bq: Long = result.head.get("count_ratings").getLongValue
  //  val sum_ratings_bq: Double = result.head.get("sum_ratings").getDoubleValue
  //
  //  "Record counts" should "be matching between PARQUET and BQ table " in {
  //    assert(count_ratings==count_records_bq)
  //  }
  //
  //  "Sum of ratings" should "be matching between PARQUET and BQ table " in {
  //    assert(sum_ratings==sum_ratings_bq)
  //  }
}


