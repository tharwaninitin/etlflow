package etlflow.task

import etlflow.SparkTestSuiteHelper
import etlflow.log.LogEnv
import etlflow.schema.Rating
import etlflow.spark.IOType.PARQUET
import etlflow.spark.{ReadApi, SparkEnv, SparkUDF, WriteApi}
import etlflow.utils.ApplicationLogger
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.ZIO
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

  val step1: SparkTask[Array[String]] = SparkTask(
    name = "LoadRatingsParquetToJdbc",
    transformFunction = getYearMonthData
  )

  def processData(ip: Array[String]): Unit = {
    logger.info("Processing Data")
    logger.info(ip.toList.toString())
  }

  def step2(ip: Array[String]): GenericTask[Unit] = GenericTask(
    name = "ProcessData",
    function = processData(ip)
  )

  val job: ZIO[SparkEnv with LogEnv, Throwable, Unit] = for {
    op <- step1.executeZio
    _  <- step2(op).executeZio
  } yield ()

  val test: ZSpec[environment.TestEnvironment with SparkEnv with LogEnv, Any] =
    testM("Execute ParquetToJdbcGenericSparkStepTestSuite step") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
