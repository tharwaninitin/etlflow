package etljobs.examples.etljob2

import etljobs.bigquery.BigQueryManager
import etljobs.etlsteps.{BQLoadStep, SparkETLStep, SparkReadTransformWriteStep, StateLessEtlStep}
import etljobs.examples.MyGlobalProperties
import etljobs.examples.schema.MyEtlJobSchema.{Rating, RatingOutput}
import etljobs.examples.schema.MyEtlJobProps.EtlJob23Props
import etljobs.spark.{ReadApi, SparkManager, SparkUDF}
import etljobs.utils.{CSV, PARQUET}
import etljobs.{EtlJob, EtlJobProps, EtlStepList}
import org.apache.spark.sql.functions.{col, from_unixtime, input_file_name}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

case class EtlJobDefinition(
                        job_name: String,
                        job_properties: EtlJobProps,
                        global_properties: Option[MyGlobalProperties]
                      )
  extends EtlJob with SparkManager with SparkUDF with BigQueryManager {
  private val gcs_output_path = f"gs://${global_properties.get.gcs_output_bucket}/output/ratings"
  private var output_date_paths : Seq[(String,String)] = Seq()
  private val temp_date_col = "temp_date_col"
  private val job_props:EtlJob23Props  = job_properties.asInstanceOf[EtlJob23Props]

  def enrichRatingData(spark: SparkSession)(in : Dataset[Rating]) : Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn(temp_date_col, get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))
        .where(f"$temp_date_col in ('20160101', '20160102')")

    ratings_df.as[RatingOutput](mapping)
  }

  def addFilePaths(spark: SparkSession, job_properties: EtlJob23Props)() = {
    import spark.implicits._

    output_date_paths = ReadApi.LoadDS[RatingOutput](Seq(gcs_output_path),PARQUET)(spark)
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
    input_type            = CSV(",", true, "FAILFAST"),
    transform_function    = enrichRatingData(spark),
    output_type           = PARQUET,
    output_location       = gcs_output_path,
    output_save_mode      = SaveMode.Overwrite,
    output_partition_col  = Seq(f"$temp_date_col"),
    output_repartitioning = true  // Setting this to true takes care of creating one file for every partition
  )(spark)

  private val step2 = new SparkETLStep(
    name               = "GenerateFilePaths",
    transform_function = addFilePaths(spark, job_props)
  )(spark) {
    override def getStepProperties(level: String) : Map[String,String] = Map("paths" -> output_date_paths.mkString(","))
  }

  private val step3 = BQLoadStep(
    name              = "LoadRatingBQ",
    input_location    = Right(output_date_paths),
    input_type        = PARQUET,
    output_dataset    = job_props.ratings_output_dataset,
    output_table      = job_props.ratings_output_table_name
  )(bq)

  val etl_step_list: List[StateLessEtlStep] = EtlStepList(step1,step2,step3)
}
