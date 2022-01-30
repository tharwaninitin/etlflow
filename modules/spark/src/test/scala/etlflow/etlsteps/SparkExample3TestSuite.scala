package etlflow.etlsteps

import etlflow.model.Credential.JDBC
import etlflow.schema.Rating
import etlflow.spark.IOType.{PARQUET, RDB}
import etlflow.spark.{ReadApi, SparkEnv, SparkUDF, WriteApi}
import etlflow.utils.ApplicationLogger
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object SparkExample3TestSuite extends SparkUDF with ApplicationLogger {

  val cred = JDBC(sys.env("DB_URL"), sys.env("DB_USER"), sys.env("DB_PWD"), sys.env("DB_DRIVER"))
  val jdbc = RDB(cred)

  val step1 = SparkReadWriteStep[Rating, Rating](
    name = "LoadRatingsParquetToJdbc",
    input_location = List("path/to/input"),
    input_type = PARQUET,
    output_type = jdbc,
    output_location = "ratings",
    output_save_mode = SaveMode.Overwrite
  )

  def getYearMonthData(spark: SparkSession): Array[String] = {
    import spark.implicits._
    val ds = ReadApi.DS[Rating](List("path/to/input"), PARQUET)(spark)
    val year_month = ds
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn("year_month", get_formatted_date("date", "yyyy-MM-dd", "yyyyMM").cast(IntegerType))
      .selectExpr("year_month")
      .distinct()
      .as[String]
      .collect()
    WriteApi.WriteDS[Rating](jdbc, "ratings")(ds, spark)
    year_month
  }

  val step2 = SparkStep(
    name = "GenerateYearMonth",
    transform_function = getYearMonthData
  )

  def processData(ip: Array[String]): Unit = {
    logger.info("Processing Data")
    logger.info(ip.toList.toString())
  }

  def step3(ip: Array[String]) = GenericETLStep(
    name = "ProcessData",
    function = processData(ip)
  )

  val job: ZIO[SparkEnv, Throwable, Unit] = for {
    _  <- step1.process
    op <- step2.process
    _  <- step3(op).process
  } yield ()

  val spec: ZSpec[environment.TestEnvironment with SparkEnv, Any] =
    suite("Spark Steps 3")(testM("Execute Basic Spark steps") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    })
}
