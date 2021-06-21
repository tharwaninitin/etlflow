package etlflow.jobs

import etlflow.TestSparkSession
import etlflow.coretests.Schema.{EtlJob2Props, EtlJobRun, Rating}
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.schema.Credential.JDBC
import etlflow.spark.{ReadApi, SparkUDF, WriteApi}
import etlflow.spark.IOType.{PARQUET, RDB}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{SaveMode, SparkSession}

case class Job2SparkReadWriteApi(job_properties: EtlJob2Props)
  extends GenericEtlJob[EtlJob2Props] with TestSparkSession with SparkUDF {

  val job_props: EtlJob2Props = job_properties
  val jdbc = RDB(JDBC(config.dbLog.url,config.dbLog.user,config.dbLog.password,config.dbLog.driver))

  val step1 = SparkReadWriteStep[Rating](
    name             = "LoadRatingsParquetToJdbc",
    input_location   = job_props.ratings_input_path,
    input_type       = PARQUET,
    output_type      = jdbc,
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
    WriteApi.WriteDS[Rating](jdbc,job_props.ratings_output_table_name)(ds,spark)
    year_month
  }

  val step2 = SparkETLStep(
    name               = "GenerateYearMonth",
    transform_function = getYearMonthData
  )

  def processData(ip: Array[String]): Unit = {
    logger.info("Processing Data")
    logger.info(ip.toList.toString())
  }

  val step3 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val step4 = DBReadStep[EtlJobRun](
    name  = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun",
    credentials = config.dbLog
  )

  def processData2(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(jr.toString))
  }

  val step5 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData2,
  )

  val job =
    for {
       _   <- step1.execute()
       op1 <- step2.execute()
       _   <- step3.execute(op1)
       op2 <- step4.execute()
       _   <- step5.execute(op2)
    } yield ()
}
