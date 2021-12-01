package etlflow.etlsteps

import etlflow.TestSparkSession
import etlflow.coretests.Schema.Rating
import etlflow.coretests.TestSuiteHelper
import etlflow.schema.Credential.JDBC
import etlflow.spark.IOType.{PARQUET, RDB}
import etlflow.spark.{ReadApi, SparkUDF, WriteApi}
import etlflow.utils.ApplicationLogger
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM}

object SparkExample3TestSuite extends DefaultRunnableSpec with TestSuiteHelper with TestSparkSession with SparkUDF with ApplicationLogger {

  val cred = JDBC(sys.env("DB_URL"), sys.env("DB_USER"), sys.env("DB_PWD"), sys.env("DB_DRIVER"))
  val jdbc = RDB(cred)

  val step1 = SparkReadWriteStep[Rating](
    name = "LoadRatingsParquetToJdbc",
    input_location = List("path/to/input"),
    input_type = PARQUET,
    output_type = jdbc,
    output_location = "ratings",
    output_save_mode = SaveMode.Overwrite
  )

  def getYearMonthData(spark: SparkSession, ip: Unit): Array[String] = {
    import spark.implicits._
    val ds = ReadApi.LoadDS[Rating](List("path/to/input"), PARQUET)(spark)
    val year_month = ds
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn("year_month", get_formatted_date("date", "yyyy-MM-dd", "yyyyMM").cast(IntegerType))
      .selectExpr("year_month").distinct().as[String].collect()
    WriteApi.WriteDS[Rating](jdbc, "ratings")(ds, spark)
    year_month
  }

  val step2 = SparkETLStep(
    name = "GenerateYearMonth",
    transform_function = getYearMonthData
  )

  def processData(ip: Array[String]): Unit = {
    logger.info("Processing Data")
    logger.info(ip.toList.toString())
  }

  val step3 = GenericETLStep(
    name = "ProcessData",
    transform_function = processData,
  )

  val job =
    for {
      _ <- step1.process(())
      op1 <- step2.process(())
      _ <- step3.process(op1)
    } yield ()

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Spark Steps")(
      testM("Execute Spark steps") {
        assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      })
}
