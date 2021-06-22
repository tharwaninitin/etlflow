package etlflow.steps.spark

import doobie.implicits._
import doobie.util.fragment.Fragment
import etlflow.TestSparkSession
import etlflow.coretests.{DoobieHelper, TestSuiteHelper}
import etlflow.coretests.Schema._
import etlflow.etlsteps.{SparkReadStep, SparkReadWriteStep}
import etlflow.schema.Credential.JDBC
import etlflow.spark.{ReadApi, SparkUDF}
import etlflow.spark.IOType.{PARQUET, RDB}
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import zio.Runtime.default.unsafeRun
import zio.Task
import zio.interop.catz._

class SparkStepTestSuite extends AnyFlatSpec
  with should.Matchers
  with DoobieHelper
  with TestSparkSession
  with SparkUDF
  with TestSuiteHelper {

  // STEP 1: Define step
  // Note: Here Parquet file has 6 columns and Rating Case Class has 4 out of those 6 columns so only 4 will be selected
  val input_path_parquet  = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet"
  val output_table        = "ratings"

  val step1 = SparkReadWriteStep[Rating](
    name             = "LoadRatingsParquetToJdbc",
    input_location   = Seq(input_path_parquet),
    input_type       = PARQUET,
    output_type      = RDB(JDBC(config.dbLog.url, config.dbLog.user, config.dbLog.password, "org.postgresql.Driver")),
    output_location  = "ratings",
    output_save_mode = SaveMode.Overwrite
  )

  val step2 = SparkReadStep[Rating](
    name             = "LoadRatingsParquet",
    input_location   = Seq(input_path_parquet),
    input_type       = PARQUET,
  )

  // STEP 2: Run Step
  unsafeRun(step1.process())
  val op: Dataset[Rating] = unsafeRun(step2.process())
  op.show(10)

  // STEP 3: Run Test
  val raw: Dataset[Rating] = ReadApi.LoadDS[Rating](Seq(input_path_parquet), PARQUET)(spark)
  val Row(sum_ratings: Double, count_ratings: Long) = raw.selectExpr("sum(rating)","count(*)").first()
  val query: String = s"SELECT sum(rating) sum_ratings, count(*) as count FROM $output_table"
  val trans = transactor(config.dbLog.url, config.dbLog.user, config.dbLog.password)
  val db_task: Task[RatingsMetrics] = Fragment.const(query).query[RatingsMetrics].unique.transact(trans)
  val db_metrics: RatingsMetrics = unsafeRun(db_task)

  "Record counts" should "be matching in transformed DF and DB table " in {
    assert(count_ratings == db_metrics.count_ratings)
  }

  "Sum of ratings" should "be matching in transformed DF and DB table " in {
    assert(sum_ratings == db_metrics.sum_ratings)
  }
}


