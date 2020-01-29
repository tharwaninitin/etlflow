package etljob2

import EtlJobSchemas.{Rating, RatingOutput}
import org.apache.spark.sql.functions.{col, from_unixtime, input_file_name}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Encoders, SparkSession, Dataset, SaveMode}
import etljobs.EtlJob
import etljobs.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep, SparkETLStep}
import etljobs.functions.SparkUDF
import etljobs.utils.{CSV, PARQUET, GlobalProperties, LOCAL}
import org.apache.log4j.{Level, Logger}
import etljobs.spark.ReadApi

class EtlJobDefinition(job_properties : Map[String,String], global_properties: Option[GlobalProperties]) extends EtlJob(job_properties, global_properties) with SparkUDF {
  var output_date_paths : Seq[(String,String)] = Seq()
  val temp_date_col = "temp_date_col"
  Logger.getLogger("org").setLevel(Level.WARN)

  def enrichRatingData(spark: SparkSession, job_properties : Map[String, String])(in : Dataset[Rating]) : Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn(temp_date_col, get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))
        .where(f"$temp_date_col in ('20160101', '20160102')")

    ratings_df.as[RatingOutput](mapping)
  }

  def addFilePaths(spark: SparkSession, job_properties : Map[String, String])() = {
    import spark.implicits._

    output_date_paths = ReadApi.LoadDS[RatingOutput](Seq(job_properties("ratings_output_path")),PARQUET)(spark)
      .select(f"$temp_date_col")
      .withColumn("filename", input_file_name)
      .distinct()
      .as[(String,String)]
      .collect()
      .map((date) => (job_properties("ratings_output_path") + f"/$temp_date_col=" + date._1 + "/" + date._2.split("/").last, date._1))

    etl_job_logger.info("Filepaths generated are: ")
    output_date_paths.foreach(path => println(path))
  }

  val step1 = SparkReadWriteStep[Rating, RatingOutput](
    name                    = "LoadRatingsParquet",
    input_location          = Seq(job_properties("ratings_input_path")),
    input_type              = CSV(",", true, job_properties.getOrElse("parse_mode","FAILFAST")),
    transform_function      = Some(enrichRatingData(spark, job_properties)),
    output_type             = PARQUET,
    output_location         = job_properties("ratings_output_path"),
    output_partition_col    = Some(f"$temp_date_col"),
    output_save_mode        = SaveMode.Overwrite,
    output_repartitioning   = true  // Setting this to true takes care of creating one file for every partition
  )(spark)

  val step2 = new SparkETLStep(
    name                    = "GenerateFilePaths",
    transform_function      = addFilePaths(spark, job_properties)
  )(spark) {
    override def getStepProperties(level: String) : Map[String,String] = Map("paths" -> output_date_paths.mkString(","))
  }

  val step3 = new BQLoadStep(
    name                    = "LoadRatingBQ",
    source_paths_partitions = output_date_paths,
    source_format           = PARQUET,
    source_file_system      = LOCAL,
    destination_dataset     = job_properties("ratings_output_dataset"),
    destination_table       = job_properties("ratings_output_table_name")
  )(bq)

  def apply() : List[EtlStep[Unit,Unit]] = List(step1,step2,step3)
}
