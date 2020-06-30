package etlflow.steps

import doobie.implicits._
import doobie.util.fragment.Fragment
import etlflow.Schema._
import etlflow.TestSuiteHelper
import etlflow.etlsteps.SparkReadWriteStep
import etlflow.spark.{ReadApi, SparkUDF}
import etlflow.utils.{JDBC, PARQUET}
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.containers.PostgreSQLContainer
import zio.Task
import zio.interop.catz._

class SparkStepTestSuite extends FlatSpec with Matchers with TestSuiteHelper with SparkUDF {

  // STEP 1: Setup test containers
  val container = new PostgreSQLContainer("postgres:latest")
  container.start()

  // STEP 2: Define step
  // Note: Here Parquet file has 6 columns and Rating Case Class has 4 out of those 6 columns so only 4 will be selected
  val input_path_parquet  = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet"
  val output_table        = "ratings"

  val step1 = SparkReadWriteStep[Rating](
    name             = "LoadRatingsParquetToJdbc",
    input_location   = Seq(input_path_parquet),
    input_type       = PARQUET,
    output_type      = JDBC(container.getJdbcUrl, container.getUsername, container.getPassword, global_props.log_db_driver),
    output_location  = "ratings",
    output_save_mode = SaveMode.Overwrite
  )

  // STEP 3: Run Step
  runtime.unsafeRun(step1.process(spark))

  // STEP 4: Run Test
  val raw: Dataset[Rating] = ReadApi.LoadDS[Rating](Seq(input_path_parquet), PARQUET)(spark)
  val Row(sum_ratings: Double, count_ratings: Long) = raw.selectExpr("sum(rating)","count(*)").first()
  val query: String = s"SELECT sum(rating) sum_ratings, count(*) as count FROM $output_table"
  val trans = transactor(container.getJdbcUrl, container.getUsername, container.getPassword)
  val db_task: Task[RatingsMetrics] = Fragment.const(query).query[RatingsMetrics].unique.transact(trans)
  val db_metrics: RatingsMetrics = runtime.unsafeRun(db_task)

  "Record counts" should "be matching in transformed DF and DB table " in {
    assert(count_ratings == db_metrics.count_ratings)
  }

  "Sum of ratings" should "be matching in transformed DF and DB table " in {
    assert(sum_ratings == db_metrics.sum_ratings)
  }
}


