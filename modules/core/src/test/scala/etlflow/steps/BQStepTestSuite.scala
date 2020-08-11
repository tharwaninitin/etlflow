package etlflow.steps

import com.google.cloud.bigquery.FieldValueList
import etlflow.etlsteps.{BQLoadStep, GCSPutStep}
import etlflow.spark.{ReadApi, SparkManager}
import etlflow.Schema._
import etlflow.utils.PARQUET
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import etlflow.TestSuiteHelper
import etlflow.gcp.{BQ, BQService}

class BQStepTestSuite extends FlatSpec with Matchers with TestSuiteHelper {
  // STEP 1: Define step
  val input_path = s"gs://$gcs_bucket/temp/ratings.parquet"
  val output_table = "ratings"
  val output_dataset = "test"

  val step1 = GCSPutStep(
    name   = "S3PutStep",
    bucket = gcs_bucket,
    key    = "temp/ratings.parquet",
    file   = file
  )

  val step2 = BQLoadStep(
    name           = "LoadRatingBQ",
    input_location = Left(input_path),
    input_type     = PARQUET,
    output_dataset = output_dataset,
    output_table   = output_table
  )

  // STEP 2: Run Step
  runtime.unsafeRun(step1.process())
  runtime.unsafeRun(step2.process())

  // STEP 3: Run Test
  private implicit val spark: SparkSession = SparkManager.createSparkSession()

  val raw: Dataset[Rating] = ReadApi.LoadDS[Rating](Seq(input_path), PARQUET)(spark)
  val Row(sum_ratings: Double, count_ratings: Long) = raw.selectExpr("sum(rating)","count(*)").first()
  val query: String = s"SELECT count(*) as count_ratings ,sum(rating) sum_ratings FROM $output_dataset.$output_table"
  val env = BQ.live()
  val result: Iterable[FieldValueList] = runtime.unsafeRun(BQService.getDataFromBQ(query).provideLayer(env))
  val count_records_bq: Long = result.head.get("count_ratings").getLongValue
  val sum_ratings_bq: Double = result.head.get("sum_ratings").getDoubleValue

  "Record counts" should "be matching between PARQUET and BQ table " in {
    assert(count_ratings==count_records_bq)
  }

  "Sum of ratings" should "be matching between PARQUET and BQ table " in {
    assert(sum_ratings==sum_ratings_bq)
  }
}


