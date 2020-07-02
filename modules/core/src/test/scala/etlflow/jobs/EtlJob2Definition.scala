package etlflow.jobs

import etlflow.{EtlJobProps, LoggerResource}
import etlflow.Schema.{EtlJob2Props, Rating}
import etlflow.etljobs.GenericEtlJobWithLogging
import etlflow.etlsteps.{GenericETLStep, SparkETLStep, SparkReadWriteStep}
import etlflow.spark.{ReadApi, SparkUDF, WriteApi}
import etlflow.utils.{GlobalProperties, PARQUET}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import zio.ZIO

case class EtlJob2Definition(job_properties: EtlJobProps, global_properties: Option[GlobalProperties])
  extends GenericEtlJobWithLogging with SparkUDF {

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

  val job: ZIO[LoggerResource, Throwable, Unit] =
    for {
      _   <- step1.execute()
      op2 <- step2.execute()
      _   <- step3.execute(op2)
    } yield ()
}
