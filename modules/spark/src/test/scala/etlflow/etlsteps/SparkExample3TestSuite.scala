package etlflow.etlsteps

import etlflow.TestSparkSession
import etlflow.coretests.Schema.{EtlJobRun, Rating}
import etlflow.coretests.TestSuiteHelper
import etlflow.schema.Credential.JDBC
import etlflow.spark.IOType.{PARQUET, RDB}
import etlflow.spark.{ReadApi, SparkUDF, WriteApi}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM}

object SparkExample3TestSuite extends DefaultRunnableSpec with TestSuiteHelper with TestSparkSession with SparkUDF {

  val jdbc = RDB(JDBC(credentials.url, credentials.user, credentials.password, credentials.driver))

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

  val step4 = DBReadStep[EtlJobRun](
    name = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun",
    credentials = credentials
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("state")))

  def processData2(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(jr.toString))
  }

  val step5 = GenericETLStep(
    name = "ProcessData",
    transform_function = processData2,
  )

  val job =
    for {
      _ <- step1.process(())
      op1 <- step2.process(())
      _ <- step3.process(op1)
      op2 <- step4.process(())
      _ <- step5.process(op2)
    } yield ()

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Spark Steps")(
      testM("Execute Spark steps") {
        assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      })
}
