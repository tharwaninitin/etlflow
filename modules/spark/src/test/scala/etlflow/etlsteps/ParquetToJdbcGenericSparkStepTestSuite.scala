package etlflow.etlsteps

import etlflow.SparkTestSuiteHelper
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

object ParquetToJdbcGenericSparkStepTestSuite extends SparkUDF with ApplicationLogger with SparkTestSuiteHelper {

  def getYearMonthData(spark: SparkSession): Array[String] = {
    import spark.implicits._
    val ds = ReadApi.DS[Rating](List(input_path_parquet), PARQUET)(spark)
    val year_month = ds
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn("year_month", get_formatted_date("date", "yyyy-MM-dd", "yyyyMM").cast(IntegerType))
      .selectExpr("year_month")
      .distinct()
      .as[String]
      .collect()
    WriteApi.DS[Rating](ds, jdbc, output_table, SaveMode.Overwrite)(spark)
    year_month
  }

  val step1: SparkStep[Array[String]] = SparkStep(
    name = "LoadRatingsParquetToJdbc",
    transform_function = getYearMonthData
  )

  def processData(ip: Array[String]): Unit = {
    logger.info("Processing Data")
    logger.info(ip.toList.toString())
  }

  def step2(ip: Array[String]): GenericETLStep[Unit] = GenericETLStep(
    name = "ProcessData",
    function = processData(ip)
  )

  val job: ZIO[SparkEnv, Throwable, Unit] = for {
    op <- step1.process
    _  <- step2(op).process
  } yield ()

  val spec: ZSpec[environment.TestEnvironment with SparkEnv, Any] =
    suite("ParquetToJdbcGenericSparkStepTestSuite")(testM("Execute ParquetToJdbc SparkStep") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    })
}
