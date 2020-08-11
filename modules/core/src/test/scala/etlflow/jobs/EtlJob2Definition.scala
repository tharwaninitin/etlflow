package etlflow.jobs

import etlflow.Schema.{EtlJob2Props, EtlJobRun, Rating}
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.spark.{ReadApi, SparkManager, SparkUDF, WriteApi}
import etlflow.utils.{Config, GlobalProperties, JDBC, PARQUET}
import etlflow.{EtlJobProps, LoggerResource}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.ZIO

case class EtlJob2Definition(job_properties: EtlJobProps,  globalProperties: Config)
  extends GenericEtlJob  with SparkUDF {

  private val global_props = globalProperties
  val job_props: EtlJob2Props = job_properties.asInstanceOf[EtlJob2Props]

  private implicit val spark: SparkSession = SparkManager.createSparkSession()


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

  val step2 = SparkETLStep(
    name               = "GenerateYearMonth",
    transform_function = getYearMonthData
  )

  def processData(ip: Array[String]): Unit = {
    etl_job_logger.info("Processing Data")
    etl_job_logger.info(ip.toList.toString())
  }

  val step3 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val step4 = DBReadStep[EtlJobRun](
    name  = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun",
    credentials = global_props.dbLog
  )

  def processData2(ip: List[EtlJobRun]): Unit = {
    etl_job_logger.info("Processing Data")
    ip.foreach(jr => etl_job_logger.info(jr.toString))
  }

  val step5 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData2,
  )

  val step6 = SparkReadStep[Rating](
    name             = "GetRatingsParquet",
    input_location   = job_props.ratings_input_path,
    input_type       = PARQUET,
  )

  val job: ZIO[LoggerResource, Throwable, Unit] =
    for {
       _   <- step1.execute()
       op1 <- step2.execute()
       _   <- step3.execute(op1)
       op2 <- step4.execute()
       _   <- step5.execute(op2)
       op3 <- step6.execute()
    } yield ()
}
