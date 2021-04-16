package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{BQLoadStep, SparkETLStep, SparkReadTransformWriteStep}
import etlflow.spark.{ReadApi, SparkManager, SparkUDF}
import examples.schema.MyEtlJobProps.EtlJob23Props
import examples.schema.MyEtlJobSchema.{Rating, RatingOutput}
import org.apache.spark.sql.functions.{col, from_unixtime, input_file_name}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import etlflow.spark.IOType
import etlflow.gcp.BQInputType

case class EtlJob2Definition(job_properties: EtlJob23Props) extends SequentialEtlJob[EtlJob23Props]  with SparkUDF {

  private val gcs_output_path = f"gs://${sys.env("GCS_BUCKET")}/output/ratings"
  private var output_date_paths : Seq[(String,String)] = Seq()
  private val temp_date_col = "temp_date_col"
  private val job_props:EtlJob23Props  = job_properties.asInstanceOf[EtlJob23Props]
  private implicit val spark: SparkSession = SparkManager.createSparkSession()

  def enrichRatingData(spark: SparkSession, in : Dataset[Rating]) : Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn(temp_date_col, get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))
        .where(f"$temp_date_col in ('20160101', '20160102')")

    ratings_df.as[RatingOutput](mapping)
  }

  def addFilePaths()(spark: SparkSession, ip: Unit): Unit = {
    import spark.implicits._

    output_date_paths = ReadApi.LoadDS[RatingOutput](Seq(gcs_output_path),IOType.PARQUET)(spark)
      .select(f"$temp_date_col")
      .withColumn("filename", input_file_name)
      .distinct()
      .as[(String,String)]
      .collect()
      .map((date) => (gcs_output_path + f"/$temp_date_col=" + date._1 + "/" + date._2.split("/").last, date._1))

    etl_job_logger.info("Filepaths generated are: ")
    output_date_paths.foreach(path => println(path))
  }

  private val step1 = SparkReadTransformWriteStep[Rating, RatingOutput](
    name                  = "LoadRatingsParquet",
    input_location        = Seq(job_props.ratings_input_path),
    input_type            = IOType.CSV(",", true, "FAILFAST"),
    transform_function    = enrichRatingData,
    output_type           = IOType.PARQUET,
    output_location       = gcs_output_path,
    output_save_mode      = SaveMode.Overwrite,
    output_partition_col  = Seq(f"$temp_date_col"),
    output_repartitioning = true  // Setting this to true takes care of creating one file for every partition
  )

  private val step2 = SparkETLStep(
    name               = "GenerateFilePaths",
    transform_function = addFilePaths()
  )
//  {
//    override def getStepProperties(level: String) : Map[String,String] = Map("paths" -> output_date_paths.mkString(","))
//  }

  private val step3 = BQLoadStep(
    name              = "LoadRatingBQ",
    input_location    = Right(output_date_paths),
    input_type        = BQInputType.PARQUET,
    output_dataset    = job_props.ratings_output_dataset,
    output_table      = job_props.ratings_output_table_name
  )

  val etlStepList = EtlStepList(step1,step2,step3)
}
