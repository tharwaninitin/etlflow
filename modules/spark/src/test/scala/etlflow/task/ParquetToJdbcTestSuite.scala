package etlflow.task

import etlflow.SparkTestSuiteHelper
import etlflow.log.LogEnv
import etlflow.schema.{Rating, RatingsMetrics}
import etlflow.spark.IOType.PARQUET
import etlflow.spark.SparkEnv
import etlflow.utils.ApplicationLogger
import org.apache.spark.sql.{Dataset, Encoders, SaveMode}
import zio.test.Assertion.equalTo
import zio.test._
import zio._

object ParquetToJdbcTestSuite extends ApplicationLogger with SparkTestSuiteHelper {

  // Note: Here Parquet file has 6 columns and Rating Case Class has 4 out of those 6 columns so only 4 will be selected
  val task1: RIO[SparkEnv with LogEnv, Unit] = SparkReadWriteTask[Rating, Rating](
    name = "LoadRatingsParquetToJdbc",
    inputLocation = List(inputPathParquet),
    inputType = PARQUET,
    outputType = jdbc,
    outputLocation = tableName,
    outputSaveMode = SaveMode.Overwrite
  ).execute

  val task2: RIO[SparkEnv with LogEnv, Dataset[Rating]] = SparkReadTask[Rating, Rating](
    name = "LoadRatingsParquet",
    inputLocation = List(inputPathParquet),
    inputType = PARQUET
  ).execute

  def task3(query: String): RIO[SparkEnv with LogEnv, Dataset[RatingsMetrics]] = SparkReadTask[RatingsMetrics, RatingsMetrics](
    name = "LoadRatingsDB",
    inputLocation = List(query),
    inputType = jdbc
  ).execute

  val job: RIO[SparkEnv with LogEnv, Boolean] = for {
    _     <- task1
    ip_ds <- task2
    enc   = Encoders.product[RatingsMetrics]
    ip    = ip_ds.selectExpr("sum(rating) as sum_ratings", "count(*) as count_ratings").as[RatingsMetrics](enc).first()
    query = s"(SELECT sum(rating) as sum_ratings, count(*) as count_ratings FROM $tableName) as T"
    op_ds <- task3(query)
    op   = op_ds.first()
    _    = logger.info(s"IP => $ip")
    _    = logger.info(s"OP => $op")
    bool = ip == op
  } yield bool

  val test: ZSpec[environment.TestEnvironment with SparkEnv with LogEnv, Any] =
    test("Record counts and sum should be matching after task run LoadRatingsParquetToJdbc")(
      assertM(job.foldZIO(ex => ZIO.fail(ex.getMessage), op => ZIO.succeed(op)))(equalTo(true))
    )
}
