package etlflow.task

import etlflow.SparkTestSuiteHelper
import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import etlflow.schema.Rating
import etlflow.spark.IOType.PARQUET
import etlflow.spark.{ReadApi, SparkEnv, SparkUDF, WriteApi}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.{Task, ZIO}
import zio.test.Assertion.equalTo
import zio.test._

object ParquetToJdbcGenericTestSuite extends SparkUDF with ApplicationLogger with SparkTestSuiteHelper {

  def getYearMonthData(spark: SparkSession): Array[String] = {
    import spark.implicits._
    val ds = ReadApi.ds[Rating](List(inputPathParquet), PARQUET)(spark)
    val yearMonth = ds
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn("year_month", getFormattedDate("date", "yyyy-MM-dd", "yyyyMM").cast(IntegerType))
      .selectExpr("year_month")
      .distinct()
      .as[String]
      .collect()
    WriteApi.ds[Rating](ds, jdbc, tableName, SaveMode.Overwrite)(spark)
    yearMonth
  }

  val task1: SparkTask[Array[String]] = SparkTask(
    name = "LoadRatingsParquetToJdbc",
    transformFunction = getYearMonthData
  )

  def processData(ip: Array[String]): Task[Unit] = ZIO.attempt {
    logger.info("Processing Data")
    logger.info(ip.toList.toString())
  }

  def task2(ip: Array[String]): GenericTask[Any, Unit] = GenericTask(
    name = "ProcessData",
    task = processData(ip)
  )

  val job: ZIO[SparkEnv with Audit, Throwable, Unit] = for {
    op <- task1.toZIO
    _  <- task2(op).toZIO
  } yield ()

  val spec: Spec[SparkEnv with Audit, Any] =
    test("Execute ParquetToJdbcGenericSparkTaskTestSuite task") {
      assertZIO(job.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
