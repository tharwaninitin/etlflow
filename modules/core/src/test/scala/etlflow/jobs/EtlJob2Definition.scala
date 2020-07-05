package etlflow.jobs

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import etlflow.{EtlJobProps, LoggerResource}
import etlflow.Schema.{EtlJob2Props, Rating}
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.{GenericETLStep, HttpMethod, HttpResponseStep, HttpStep, SendMailStep, SparkETLStep, SparkReadWriteStep}
import etlflow.spark.{ReadApi, SparkManager, SparkUDF, WriteApi}
import etlflow.utils.{GlobalProperties, PARQUET}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.ZIO

case class EtlJob2Definition(job_properties: EtlJobProps, global_properties: Option[GlobalProperties])
  extends GenericEtlJob with SparkManager with SparkUDF {

  val job_props: EtlJob2Props = job_properties.asInstanceOf[EtlJob2Props]

  val step1 = SparkReadWriteStep[Rating](
    name             = "LoadRatingsParquetToJdbc",
    input_location   = job_props.ratings_input_path,
    input_type       = PARQUET,
    output_type      = job_props.ratings_output_type,
    output_location  = job_props.ratings_output_table_name,
    output_save_mode = SaveMode.Overwrite
  )

  def getYearMonthData(spark: SparkSession, ip: Unit): Array[String] = {
    import spark.implicits._
    val ds = ReadApi.LoadDS[Rating](job_props.ratings_input_path,PARQUET)(spark)
    val year_month = ds
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn("year_month", get_formatted_date("date","yyyy-MM-dd","yyyyMM").cast(IntegerType))
      .selectExpr("year_month").distinct().as[String].collect()
    WriteApi.WriteDS[Rating](job_props.ratings_output_type,job_props.ratings_output_table_name)(ds,spark)
    year_month
  }

  def processData(ip: Array[String]): Unit = {
    etl_job_logger.info("Processing Data")
    etl_job_logger.info(ip.toList.toString())
  }

  val step2 = SparkETLStep(
    name               = "GenerateYearMonth",
    transform_function = getYearMonthData
  )

  val step3 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val step4 = HttpStep(
    name         = "HttpGetSimple",
    url          = "https://httpbin.org/get",
    http_method  = HttpMethod.GET,
    log_response = true,
  )

  val step5 = HttpResponseStep(
    name         = "HttpGetParams",
    url          = "https://httpbin.org/get",
    http_method  = HttpMethod.GET,
    params       = Right(Seq(("param1","value1"))),
    log_response = true,
  )

  val step6 = HttpStep(
    name         = "HttpPostJson",
    url          = "https://httpbin.org/post",
    http_method  = HttpMethod.POST,
    params       = Left("""{"key":"value"}"""),
    headers      = Map("content-type"->"application/json"),
    log_response = true,
  )

  val step7 = HttpResponseStep(
    name         = "HttpPostParams",
    url          = "https://httpbin.org/post",
    http_method  = HttpMethod.POST,
    params       = Right(Seq(("param1","value1"))),
    log_response = true,
  )

  val emailBody: String = {
    val exec_time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
    s"""
       | SMTP Email Test
       | Time of Execution: $exec_time
       |""".stripMargin
  }

  val step8 = SendMailStep(
    name           = "SendSMTPEmail",
    body           = emailBody,
    subject        = "EtlFlow Test Ran Successfully",
    recipient_list = List(sys.env.getOrElse("SMTP_RECIPIENT","...")),
    credentials    = job_props.smtp_creds
  )

  val job: ZIO[LoggerResource, Throwable, Unit] =
    for {
       _   <- step1.execute()
       op2 <- step2.execute()
       _   <- step3.execute(op2)
       _   <- step4.execute()
       _   <- step5.execute()
       _   <- step6.execute()
       _   <- step7.execute()
       _   <- step8.execute()
    } yield ()
}
